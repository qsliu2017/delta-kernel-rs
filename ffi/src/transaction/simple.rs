//! Simple field-based APIs for adding files to Delta transactions.
//!
//! This module provides easy-to-use C FFI functions that don't require
//! Arrow knowledge, making Delta Kernel more accessible to C/C++ consumers.

use std::os::raw::c_char;
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, Int64Builder, ListBuilder, MapBuilder,
    MapFieldNames, RecordBatch, StringArray, StringBuilder, StructArray,
};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{PrimitiveType, StructField, StructType};
use delta_kernel::transaction::{add_files_schema, remove_files_schema};
use delta_kernel::{DeltaResult, EngineData, Error};
use serde_json::to_string;
use url::Url;

use crate::error::{ExternResult, IntoExternResult};
use crate::SharedExternEngine;
use crate::{
    handle::Handle, transaction::ExclusiveTransaction, unwrap_and_parse_path_as_url, ExternEngine,
    KernelStringSlice, TryFromStringSlice,
};

#[repr(C)]
pub struct AddFileActionMetadata {
    /// Safety: non null. A null-terminated string.
    pub path: NonNull<c_char>,
    // partitions: HashMap<String, String>
    pub size: u64,
    pub modification_time: i64,
    pub data_change: bool,
    pub num_records: i64,
    /// Safety: nullable.
    pub deletion_vector: Option<NonNull<DeletionVectorDescriptor>>,
}

/// Deletion vector descriptor. The same as [`DeletionVectorDescriptor`] in kernel.
///
/// [`DeletionVectorDescriptor`]: delta_kernel::actions::deletion_vector::DeletionVectorDescriptor
#[repr(C)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: c_char,
    /// Safety: non null. A null-terminated string.
    pub path_or_inline_dv: NonNull<c_char>,
    /// Safety: nullable. If null, the offset is 0.
    pub offset: Option<NonNull<i32>>,
    pub size_in_bytes: i32,
    pub cardinality: i64,
}

/// Metadata for a single `remove` action exposed via the simple C FFI.
///
/// This mirrors a subset of the Delta Lake `remove` action schema in a
/// field-oriented layout suitable for FFI callers. Optional values are
/// represented as nullable pointers.
#[repr(C)]
pub struct RemoveFileActionMetadata {
    /// Safety: non null. A null-terminated string.
    pub path: NonNull<c_char>,
    /// Milliseconds since epoch.
    ///
    /// Safety: nullable.
    pub deletion_timestamp: Option<NonNull<i64>>,
    /// Required.
    pub data_change: bool,
    /// Safety: nullable. When null, treated as false.
    pub extended_file_metadata: Option<NonNull<bool>>,
    /// Size in bytes.
    ///
    /// Safety: nullable.
    pub size: Option<NonNull<i64>>,
    /// Safety: nullable.
    pub deletion_vector: Option<NonNull<DeletionVectorDescriptor>>,
}

/// Protocol metadata required to create an initial Delta log entry.
#[repr(C)]
pub struct ProtocolActionSimple {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    /// Safety: nullable when len is 0.
    pub reader_features: *const KernelStringSlice,
    pub reader_features_len: usize,
    /// Safety: nullable when len is 0.
    pub writer_features: *const KernelStringSlice,
    pub writer_features_len: usize,
}

/// Table metadata required to create an initial Delta log entry.
#[repr(C)]
pub struct MetadataActionSimple {
    /// Safety: non null. A null-terminated string.
    pub id: NonNull<c_char>,
    /// Safety: nullable. A null-terminated string when present.
    pub name: Option<NonNull<c_char>>,
    /// Safety: nullable. A null-terminated string when present.
    pub description: Option<NonNull<c_char>>,
    /// Safety: nullable when len is 0.
    pub partition_columns: *const KernelStringSlice,
    pub partition_columns_len: usize,
    /// Safety: nullable when len is 0.
    pub configuration_keys: *const KernelStringSlice,
    /// Safety: nullable when len is 0.
    pub configuration_values: *const KernelStringSlice,
    pub configuration_len: usize,
    /// Safety: nullable.
    pub created_time: Option<NonNull<i64>>,
    /// Safety: nullable. Defaults to "parquet" when absent.
    pub format_provider: Option<NonNull<c_char>>,
}

/// Primitive column types supported by the simple schema initializer.
#[repr(C)]
#[derive(Copy, Clone)]
#[allow(dead_code)]
pub enum SimpleType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float64 = 3,
    Utf8 = 4,
}

/// C-friendly representation of a table column.
#[repr(C)]
pub struct SimpleField {
    pub name: KernelStringSlice,
    pub ty: SimpleType,
    pub nullable: bool,
}

unsafe fn cloned_c_str(ptr: *const c_char) -> DeltaResult<String> {
    let c_str = std::ffi::CStr::from_ptr(ptr);
    Ok(c_str
        .to_str()
        .map_err(|_| Error::generic("Failed to convert path to string"))?
        .to_owned())
}

unsafe fn optional_c_string(ptr: Option<NonNull<c_char>>) -> DeltaResult<Option<String>> {
    ptr.map(|p| cloned_c_str(p.as_ptr())).transpose()
}

unsafe fn slices_to_strings(ptr: *const KernelStringSlice, len: usize) -> DeltaResult<Vec<String>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(Error::generic(
            "received null pointer for string slice array",
        ));
    }
    let slices = slice::from_raw_parts(ptr, len);
    slices
        .iter()
        .map(|slice| unsafe { String::try_from_slice(slice) })
        .collect()
}

fn non_null_string_list(values: &[String]) -> DeltaResult<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for value in values {
        builder.values().append_value(value);
    }
    let _ = builder.append(true);
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn nullable_string_list(values: Option<Vec<String>>) -> DeltaResult<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    match values {
        Some(values) => {
            for value in values {
                builder.values().append_value(value);
            }
            let _ = builder.append(true);
        }
        None => {
            let _ = builder.append(false);
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn string_map_builder(nullable_values: bool) -> MapBuilder<StringBuilder, StringBuilder> {
    MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new(
        "value".to_string(),
        DataType::Utf8,
        nullable_values,
    ))
}

fn string_map_array(pairs: &[(String, String)], nullable_values: bool) -> DeltaResult<ArrayRef> {
    let mut builder = string_map_builder(nullable_values);
    for (key, value) in pairs {
        builder.keys().append_value(key);
        builder.values().append_value(value);
    }
    let _ = builder.append(true);
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn string_map_data_type(nullable_values: bool) -> DataType {
    let key_field = Field::new("key", DataType::Utf8, false);
    let value_field = Field::new("value", DataType::Utf8, nullable_values);
    let entry_struct = Field::new(
        "entries",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    );
    DataType::Map(entry_struct.into(), false)
}

fn map_simple_type(ty: SimpleType) -> PrimitiveType {
    match ty {
        SimpleType::Boolean => PrimitiveType::Boolean,
        SimpleType::Int32 => PrimitiveType::Integer,
        SimpleType::Int64 => PrimitiveType::Long,
        SimpleType::Float64 => PrimitiveType::Double,
        SimpleType::Utf8 => PrimitiveType::String,
    }
}

fn build_struct_type_from_simple_fields(
    fields_ptr: *const SimpleField,
    fields_len: usize,
) -> DeltaResult<StructType> {
    if fields_len == 0 {
        return Ok(StructType::new(vec![]));
    }
    if fields_ptr.is_null() {
        return Err(Error::generic(
            "simple schema pointer must not be null when field count is non-zero",
        ));
    }
    let fields = unsafe { slice::from_raw_parts(fields_ptr, fields_len) };
    let mut struct_fields = Vec::with_capacity(fields_len);
    for field in fields {
        let name = unsafe { String::try_from_slice(&field.name)? };
        let data_type = map_simple_type(field.ty);
        let struct_field = if field.nullable {
            StructField::nullable(name, data_type)
        } else {
            StructField::not_null(name, data_type)
        };
        struct_fields.push(struct_field);
    }
    Ok(StructType::new(struct_fields))
}

fn protocol_fields() -> Vec<Field> {
    vec![
        Field::new("minReaderVersion", DataType::Int32, false),
        Field::new("minWriterVersion", DataType::Int32, false),
        Field::new(
            "readerFeatures",
            DataType::List(Field::new("item", DataType::Utf8, true).into()),
            true,
        ),
        Field::new(
            "writerFeatures",
            DataType::List(Field::new("item", DataType::Utf8, true).into()),
            true,
        ),
    ]
}

fn format_fields() -> Vec<Field> {
    vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("options", string_map_data_type(false), false),
    ]
}

fn metadata_fields() -> Vec<Field> {
    vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("format", DataType::Struct(format_fields().into()), false),
        Field::new("schemaString", DataType::Utf8, false),
        Field::new(
            "partitionColumns",
            DataType::List(Field::new("item", DataType::Utf8, true).into()),
            false,
        ),
        Field::new("createdTime", DataType::Int64, true),
        Field::new("configuration", string_map_data_type(false), false),
    ]
}

fn log_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("protocol", DataType::Struct(protocol_fields().into()), true),
        Field::new("metaData", DataType::Struct(metadata_fields().into()), true),
    ]))
}

fn commit_url(table_root: &Url) -> DeltaResult<Url> {
    let mut root_str = table_root.to_string();
    if !root_str.ends_with('/') {
        root_str.push('/');
    }
    let normalized_root = Url::parse(&root_str)
        .map_err(|e| Error::generic(format!("failed to normalize table root: {e}")))?;
    normalized_root
        .join(&format!("_delta_log/{:020}.json", 0))
        .map_err(|e| Error::generic(format!("failed to resolve commit file: {e}")))
}

fn initialize_table_simple_impl(
    url: DeltaResult<Url>,
    extern_engine: &dyn ExternEngine,
    fields_ptr: *const SimpleField,
    fields_len: usize,
    protocol: &ProtocolActionSimple,
    metadata: &MetadataActionSimple,
) -> DeltaResult<u64> {
    let table_root = url?;
    let commit_path = commit_url(&table_root)?;
    let logical_schema = build_struct_type_from_simple_fields(fields_ptr, fields_len)?;
    let schema_json = to_string(&logical_schema)?;
    let log_schema = log_schema();
    let protocol_action = build_protocol_action(log_schema.clone(), protocol)?;
    let metadata_action = build_metadata_action(log_schema, metadata, &schema_json)?;
    let actions = vec![Ok(protocol_action), Ok(metadata_action)];

    let engine = extern_engine.engine();
    let json_handler = engine.json_handler();
    json_handler.write_json_file(&commit_path, Box::new(actions.into_iter()), false)?;
    Ok(0)
}

/// Write the initial protocol and metadata actions directly to the Delta log.
///
/// # Safety
///
/// Caller must pass valid pointers and handles. The target table directory must exist and not
/// already contain a commit.
#[no_mangle]
pub unsafe extern "C" fn initialize_table_simple(
    path: KernelStringSlice,
    extern_engine: Handle<SharedExternEngine>,
    fields: *const SimpleField,
    fields_len: usize,
    protocol: &ProtocolActionSimple,
    metadata: &MetadataActionSimple,
) -> ExternResult<u64> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let extern_engine = unsafe { extern_engine.as_ref() };
    initialize_table_simple_impl(url, extern_engine, fields, fields_len, protocol, metadata)
        .into_extern_result(&extern_engine)
}

fn build_protocol_action(
    schema: Arc<ArrowSchema>,
    action: &ProtocolActionSimple,
) -> DeltaResult<Box<dyn EngineData>> {
    let reader_features = unsafe {
        let features = slices_to_strings(action.reader_features, action.reader_features_len)?;
        if features.is_empty() {
            None
        } else {
            Some(features)
        }
    };
    let writer_features = unsafe {
        let features = slices_to_strings(action.writer_features, action.writer_features_len)?;
        if features.is_empty() {
            None
        } else {
            Some(features)
        }
    };

    let protocol_struct = Arc::new(StructArray::try_new_with_length(
        protocol_fields().into(),
        vec![
            Arc::new(Int32Array::from(vec![action.min_reader_version])) as ArrayRef,
            Arc::new(Int32Array::from(vec![action.min_writer_version])) as ArrayRef,
            nullable_string_list(reader_features)?,
            nullable_string_list(writer_features)?,
        ],
        None,
        1,
    )?) as ArrayRef;

    let metadata_struct = Arc::new(StructArray::new_null(metadata_fields().into(), 1)) as ArrayRef;

    let record_batch = RecordBatch::try_new(schema, vec![protocol_struct, metadata_struct])?;
    Ok(Box::new(ArrowEngineData::new(record_batch)))
}

fn build_metadata_action(
    schema: Arc<ArrowSchema>,
    action: &MetadataActionSimple,
    schema_json: &str,
) -> DeltaResult<Box<dyn EngineData>> {
    let id = Arc::new(StringArray::from(vec![unsafe {
        cloned_c_str(action.id.as_ptr())?
    }])) as ArrayRef;

    let mut name_builder = StringBuilder::new();
    if let Some(name_ptr) = action.name {
        let name_str = unsafe { cloned_c_str(name_ptr.as_ptr())? };
        name_builder.append_value(&name_str);
    } else {
        name_builder.append_null();
    }
    let name = Arc::new(name_builder.finish()) as ArrayRef;

    let mut description_builder = StringBuilder::new();
    if let Some(desc_ptr) = action.description {
        let description_str = unsafe { cloned_c_str(desc_ptr.as_ptr())? };
        description_builder.append_value(&description_str);
    } else {
        description_builder.append_null();
    }
    let description = Arc::new(description_builder.finish()) as ArrayRef;

    let format_provider = Arc::new(StringArray::from(vec![unsafe {
        optional_c_string(action.format_provider)?.unwrap_or_else(|| "parquet".to_string())
    }])) as ArrayRef;
    let format_options = string_map_array(&[], false)?;
    let format_struct = Arc::new(StructArray::try_new_with_length(
        format_fields().into(),
        vec![format_provider, format_options],
        None,
        1,
    )?) as ArrayRef;

    let schema_string = Arc::new(StringArray::from(vec![schema_json.to_string()])) as ArrayRef;

    let partition_columns_strings =
        unsafe { slices_to_strings(action.partition_columns, action.partition_columns_len)? };
    let partition_columns = non_null_string_list(&partition_columns_strings)?;

    let created_time = {
        let mut builder = Int64Builder::new();
        if let Some(ptr) = action.created_time {
            let _ = builder.append_value(unsafe { *ptr.as_ptr() });
        } else {
            let _ = builder.append_null();
        }
        Arc::new(builder.finish()) as ArrayRef
    };

    let configuration_pairs = unsafe {
        let keys = slices_to_strings(action.configuration_keys, action.configuration_len)?;
        let values = slices_to_strings(action.configuration_values, action.configuration_len)?;
        if keys.len() != values.len() {
            return Err(Error::generic(
                "configuration keys and values arrays must have the same length",
            ));
        }
        keys.into_iter().zip(values.into_iter()).collect::<Vec<_>>()
    };
    let configuration = string_map_array(&configuration_pairs, false)?;

    let metadata_struct = Arc::new(StructArray::try_new_with_length(
        metadata_fields().into(),
        vec![
            id,
            name,
            description,
            format_struct,
            schema_string,
            partition_columns,
            created_time,
            configuration,
        ],
        None,
        1,
    )?) as ArrayRef;

    let protocol_struct = Arc::new(StructArray::new_null(protocol_fields().into(), 1)) as ArrayRef;

    let record_batch = RecordBatch::try_new(schema, vec![protocol_struct, metadata_struct])?;
    Ok(Box::new(ArrowEngineData::new(record_batch)))
}

/// Add a single file to the transaction with simple field-based parameters.
/// This is a simplified API that doesn't require Arrow knowledge.
///
/// # Safety
///
/// Caller is responsible for passing valid handles and string pointers.
/// String pointers must be valid UTF-8 and null-terminated.
#[no_mangle]
pub unsafe extern "C" fn add_file_simple(
    mut txn: Handle<ExclusiveTransaction>,
    extern_engine: Handle<SharedExternEngine>,
    metadata: &AddFileActionMetadata,
) -> ExternResult<u64> {
    let txn = unsafe { txn.as_mut() };
    let extern_engine = unsafe { extern_engine.as_ref() };
    metadata
        .as_record_batch()
        .and_then(|record_batch| {
            txn.add_files(record_batch);
            Ok(0)
        })
        .into_extern_result(&extern_engine)
}

#[no_mangle]
pub unsafe extern "C" fn remove_file_simple(
    mut txn: Handle<ExclusiveTransaction>,
    extern_engine: Handle<SharedExternEngine>,
    metadata: &RemoveFileActionMetadata,
) -> ExternResult<u64> {
    let txn = unsafe { txn.as_mut() };
    let extern_engine = unsafe { extern_engine.as_ref() };
    metadata
        .as_record_batch()
        .and_then(|record_batch| {
            txn.remove_files(record_batch);
            Ok(0)
        })
        .into_extern_result(&extern_engine)
}

impl AddFileActionMetadata {
    /// Convert the AddFile into a record batch which matches the schema returned by
    /// [`add_files_schema`].
    ///
    /// [`add_files_schema`]: delta_kernel::transaction::add_files_schema
    unsafe fn as_record_batch(&self) -> DeltaResult<Box<ArrowEngineData>> {
        // copy C string to Rust string
        let path = unsafe { cloned_c_str(self.path.as_ptr())? };
        let path = Arc::new(StringArray::from(vec![path]));

        let key_builder = StringBuilder::new();
        let val_builder = StringBuilder::new();
        let names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut builder = MapBuilder::new(Some(names), key_builder, val_builder);
        builder.append(true)?;
        let partitions = Arc::new(builder.finish());

        let size: i64 = self
            .size
            .try_into()
            .map_err(|_| Error::generic("Failed to convert size to i64"))?;
        let size = Arc::new(Int64Array::from(vec![size]));
        let modification_time = Arc::new(Int64Array::from(vec![self.modification_time]));
        let data_change = Arc::new(BooleanArray::from(vec![self.data_change]));

        let stats = Arc::new(StructArray::try_new_with_length(
            vec![Field::new("numRecords", DataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![self.num_records]))],
            None,
            1,
        )?);

        let deletion_vector = if let Some(desc) = self.deletion_vector {
            let dv = unsafe { desc.read() };
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec![
                    (dv.storage_type as u8 as char).to_string()
                ])),
                Arc::new(StringArray::from(vec![unsafe {
                    cloned_c_str(dv.path_or_inline_dv.as_ptr())?
                }])),
                Arc::new(Int32Array::from(vec![dv
                    .offset
                    .map(|v| unsafe { v.read() })
                    .unwrap_or(0)])),
                Arc::new(Int32Array::from(vec![dv.size_in_bytes])),
                Arc::new(Int64Array::from(vec![dv.cardinality])),
            ];

            Arc::new(StructArray::try_new_with_length(
                vec![
                    Field::new("storageType", DataType::Utf8, false),
                    Field::new("pathOrInlineDv", DataType::Utf8, false),
                    Field::new("offset", DataType::Int32, true),
                    Field::new("sizeInBytes", DataType::Int32, false),
                    Field::new("cardinality", DataType::Int64, false),
                ]
                .into(),
                arrays,
                None,
                1,
            )?)
        } else {
            // Create the deletion vector struct array (null)
            Arc::new(StructArray::new_null(
                vec![
                    Field::new("storageType", DataType::Utf8, false),
                    Field::new("pathOrInlineDv", DataType::Utf8, false),
                    Field::new("offset", DataType::Int32, true),
                    Field::new("sizeInBytes", DataType::Int32, false),
                    Field::new("cardinality", DataType::Int64, false),
                ]
                .into(),
                1,
            ))
        };

        Ok(Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(add_files_schema().as_ref().try_into_arrow()?),
            vec![
                path,
                partitions,
                size,
                modification_time,
                data_change,
                stats,
                deletion_vector,
            ],
        )?)))
    }
}

impl RemoveFileActionMetadata {
    /// Convert the RemoveFile into a record batch which matches the schema returned by
    /// [`remove_files_schema`].
    ///
    /// [`remove_files_schema`]: delta_kernel::transaction::remove_files_schema
    unsafe fn as_record_batch(&self) -> DeltaResult<Box<ArrowEngineData>> {
        let path = Arc::new(StringArray::from(vec![unsafe {
            cloned_c_str(self.path.as_ptr())?
        }]));

        let deletion_timestamp = Arc::new(Int64Array::from(vec![self
            .deletion_timestamp
            .map(|v| unsafe { v.read() })
            .unwrap_or(0)]));

        let data_change = Arc::new(BooleanArray::from(vec![self.data_change]));

        let extended_file_metadata = Arc::new(BooleanArray::from(vec![self
            .extended_file_metadata
            .map(|v| unsafe { v.read() })
            .unwrap_or(false)]));

        let key_builder = StringBuilder::new();
        let val_builder = StringBuilder::new();
        let names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut builder = MapBuilder::new(Some(names), key_builder, val_builder);
        builder.append(true)?;
        let partition_values = Arc::new(builder.finish());

        let size = Arc::new(Int64Array::from(vec![self
            .size
            .map(|v| unsafe { v.read() })
            .unwrap_or(0)]));

        let deletion_vector = if let Some(desc) = self.deletion_vector {
            let dv = unsafe { desc.read() };
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec![
                    (dv.storage_type as u8 as char).to_string()
                ])),
                Arc::new(StringArray::from(vec![unsafe {
                    cloned_c_str(dv.path_or_inline_dv.as_ptr())?
                }])),
                Arc::new(Int32Array::from(vec![dv
                    .offset
                    .map(|v| unsafe { v.read() })
                    .unwrap_or(0)])),
                Arc::new(Int32Array::from(vec![dv.size_in_bytes])),
                Arc::new(Int64Array::from(vec![dv.cardinality])),
            ];

            Arc::new(StructArray::try_new_with_length(
                vec![
                    Field::new("storageType", DataType::Utf8, false),
                    Field::new("pathOrInlineDv", DataType::Utf8, false),
                    Field::new("offset", DataType::Int32, true),
                    Field::new("sizeInBytes", DataType::Int32, false),
                    Field::new("cardinality", DataType::Int64, false),
                ]
                .into(),
                arrays,
                None,
                1,
            )?)
        } else {
            // Create the deletion vector struct array (null)
            Arc::new(StructArray::new_null(
                vec![
                    Field::new("storageType", DataType::Utf8, false),
                    Field::new("pathOrInlineDv", DataType::Utf8, false),
                    Field::new("offset", DataType::Int32, true),
                    Field::new("sizeInBytes", DataType::Int32, false),
                    Field::new("cardinality", DataType::Int64, false),
                ]
                .into(),
                1,
            ))
        };

        Ok(Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(remove_files_schema().as_ref().try_into_arrow()?),
            vec![
                path,
                deletion_timestamp,
                data_change,
                extended_file_metadata,
                partition_values,
                size,
                deletion_vector,
            ],
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::arrow::array::{
        Array, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    };
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::schema::{PrimitiveType, StructField, StructType};
    use std::ffi::CString;
    use std::ptr::NonNull;

    #[test]
    fn protocol_action_builder_creates_expected_row() {
        let reader_feature = String::from("delta");
        let reader_slice = unsafe { KernelStringSlice::new_unsafe(reader_feature.as_str()) };
        let reader_slices = vec![reader_slice];

        let action = ProtocolActionSimple {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: reader_slices.as_ptr(),
            reader_features_len: reader_slices.len(),
            writer_features: std::ptr::null(),
            writer_features_len: 0,
        };

        let schema = log_schema();
        let engine_data = build_protocol_action(schema.clone(), &action).expect("protocol action");
        let arrow_data = ArrowEngineData::try_from_engine_data(engine_data).expect("arrow data");
        let batch = arrow_data.record_batch();

        let protocol = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("protocol struct");
        let min_reader = protocol
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("min reader");
        assert_eq!(min_reader.value(0), 1);

        let reader_features = protocol
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("reader features");
        assert!(!reader_features.is_null(0));
        let reader_values = reader_features.value(0);
        let reader_values = reader_values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("reader values");
        assert_eq!(reader_values.value(0), "delta");

        let metadata = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("metadata struct");
        assert!(metadata.is_null(0));
    }

    #[test]
    fn metadata_action_builder_creates_expected_row() {
        let id = CString::new("table-id").unwrap();
        let mut created_time = 123_i64;

        let partition_strings = vec![String::from("country"), String::from("date")];
        let partition_slices: Vec<_> = partition_strings
            .iter()
            .map(|s| unsafe { KernelStringSlice::new_unsafe(s.as_str()) })
            .collect();

        let config_keys = vec![String::from("delta.appendOnly")];
        let config_values = vec![String::from("true")];
        let config_key_slices: Vec<_> = config_keys
            .iter()
            .map(|s| unsafe { KernelStringSlice::new_unsafe(s.as_str()) })
            .collect();
        let config_value_slices: Vec<_> = config_values
            .iter()
            .map(|s| unsafe { KernelStringSlice::new_unsafe(s.as_str()) })
            .collect();

        let metadata = MetadataActionSimple {
            id: NonNull::new(id.as_ptr() as *mut c_char).unwrap(),
            name: None,
            description: None,
            partition_columns: partition_slices.as_ptr(),
            partition_columns_len: partition_slices.len(),
            configuration_keys: config_key_slices.as_ptr(),
            configuration_values: config_value_slices.as_ptr(),
            configuration_len: config_key_slices.len(),
            created_time: NonNull::new(&mut created_time as *mut i64),
            format_provider: None,
        };

        let schema = log_schema();
        let logical_schema =
            StructType::new(vec![StructField::not_null("value", PrimitiveType::Long)]);
        let schema_json = to_string(&logical_schema).expect("schema json");
        let engine_data = build_metadata_action(schema.clone(), &metadata, &schema_json)
            .expect("metadata action");
        let arrow_data = ArrowEngineData::try_from_engine_data(engine_data).expect("arrow data");
        let batch = arrow_data.record_batch();

        let protocol = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("protocol struct");
        assert!(protocol.is_null(0));

        let metadata_struct = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("metadata struct");
        assert!(!metadata_struct.is_null(0));

        let id_array = metadata_struct
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id array");
        assert_eq!(id_array.value(0), "table-id");

        let partition_columns = metadata_struct
            .column(5)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("partition columns");
        let partition_values = partition_columns.value(0);
        let partition_values = partition_values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("partition values");
        assert_eq!(partition_values.value(0), "country");
        assert_eq!(partition_values.value(1), "date");

        let created_time_array = metadata_struct
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("created time");
        assert_eq!(created_time_array.value(0), 123);

        let configuration = metadata_struct
            .column(7)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("configuration");
        assert_eq!(configuration.value_length(0), 1);
        let entries = configuration.value(0);
        let entries = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("configuration entries");
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("config keys");
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("config values");
        assert_eq!(keys.value(0), "delta.appendOnly");
        assert_eq!(values.value(0), "true");
    }
}
