//! Simple field-based APIs for adding files to Delta transactions.
//!
//! This module provides easy-to-use C FFI functions that don't require
//! Arrow knowledge, making Delta Kernel more accessible to C/C++ consumers.

use std::os::raw::c_char;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    BooleanArray, Int64Array, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    StructArray,
};
use delta_kernel::arrow::datatypes::{DataType, Field};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::transaction::add_files_schema;
use delta_kernel::Error;
use delta_kernel::{engine::arrow_data::ArrowEngineData, DeltaResult};

use crate::error::{ExternResult, IntoExternResult};
use crate::SharedExternEngine;
use crate::{handle::Handle, transaction::ExclusiveTransaction};

#[repr(C)]
pub struct AddFileActionMetadata {
    path: *const c_char,
    // partitions: HashMap<String, String>
    size: u64,
    modification_time: i64,
    data_change: bool,
    // stats: { num_records: i64 }
    /// Nullable.
    deletion_vector: *const DeletionVectorDescriptor,
}

/// Deletion vector descriptor. The same as [`DeletionVectorDescriptor`] in kernel.
///
/// [`DeletionVectorDescriptor`]: delta_kernel::actions::deletion_vector::DeletionVectorDescriptor
#[repr(C)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    storage_type: c_char,
    /// Not null. A null-terminated string.
    path_or_inline_dv: *const c_char,
    /// Nullable. If null, the offset is 0.
    offset: *const i32,
    size_in_bytes: i32,
    cardinality: i64,
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

impl AddFileActionMetadata {
    /// Convert the AddFile into a record batch which matches the schema returned by
    /// [`add_files_schema`].
    ///
    /// [`add_files_schema`]: delta_kernel::transaction::add_files_schema
    fn as_record_batch(&self) -> DeltaResult<Box<ArrowEngineData>> {
        // copy C string to Rust string
        let path = unsafe { std::ffi::CStr::from_ptr(self.path) };
        let path = path
            .to_str()
            .map_err(|_| Error::generic("Failed to convert path to string"))?
            .to_owned();
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
            vec![Arc::new(Int64Array::from(vec![0 as i64]))],
            None,
            1,
        )?);

        Ok(Box::new(ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(add_files_schema().as_ref().try_into_arrow()?),
            vec![
                path,
                partitions,
                size,
                modification_time,
                data_change,
                stats,
            ],
        )?)))
    }
}
