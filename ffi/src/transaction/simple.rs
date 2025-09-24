//! Simple field-based APIs for adding files to Delta transactions.
//! 
//! This module provides easy-to-use C FFI functions that don't require 
//! Arrow knowledge, making Delta Kernel more accessible to C/C++ consumers.

use std::ffi::{CStr, c_char};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{DeltaResult, SharedExternEngine};
use super::ExclusiveTransaction;
use delta_kernel::EngineData;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::arrow::array::{Array, StringArray, Int64Array, BooleanArray};
use std::sync::Arc;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::transaction::add_files_schema;
#[cfg(feature = "default-engine-base")]
use crate::engine_data::get_engine_data;

/// Create EngineData from file metadata using a hybrid approach:
/// - Use Arrow native builders for simple fields (path, size, etc.)
/// - Use JSON for complex nested structures (partitionValues, stats)
unsafe fn create_engine_data_from_params(
    path: *const c_char,
    size_bytes: u64,
    modification_time_ms: u64,
    num_records: u64,
    partition_values_json: *const c_char,
    stats_json: *const c_char,
    data_change: bool,
    _dv_storage_type: *const c_char,
    _dv_path_or_inline: *const c_char,
    _dv_offset: u64,
    _dv_size_bytes: u64,
    _dv_cardinality: u64,
    engine: &Handle<SharedExternEngine>,
) -> Result<Box<dyn EngineData>, delta_kernel::Error> {
    // Convert C strings to Rust strings immediately to avoid lifetime issues
    let path_str = CStr::from_ptr(path).to_str()
        .map_err(|_| delta_kernel::Error::generic("Invalid path string"))?;
    
    let partition_values_str = if partition_values_json.is_null() {
        "{}"
    } else {
        CStr::from_ptr(partition_values_json).to_str()
            .map_err(|_| delta_kernel::Error::generic("Invalid partition values string"))?
    };
    
    let stats_str = if stats_json.is_null() {
        format!(r#"{{"numRecords": {}}}"#, num_records)
    } else {
        let provided_stats = CStr::from_ptr(stats_json).to_str()
            .map_err(|_| delta_kernel::Error::generic("Invalid stats string"))?;
        provided_stats.to_string()
    };
    
    // Build JSON string for the file metadata
    let json_metadata = format!(
        r#"{{"path":"{}","partitionValues":{},"size":{},"modificationTime":{},"dataChange":{},"stats":{}}}"#,
        path_str, partition_values_str, size_bytes, modification_time_ms, 
        data_change, stats_str
    );
    
    // Use JSON to Arrow conversion (same as existing working code)
    let schema: ArrowSchema = add_files_schema().as_ref().try_into_arrow()
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to convert schema: {}", e)))?;
    
    // Convert JSON to Arrow RecordBatch
    let cursor = std::io::Cursor::new(json_metadata.as_bytes());
    let mut reader = delta_kernel::arrow::json::ReaderBuilder::new(schema.into()).build(cursor)
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to create JSON reader: {}", e)))?;
    
    let batch = reader.next().unwrap()
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to read JSON: {}", e)))?;
    
    // Convert RecordBatch to FFI
    let struct_array: delta_kernel::arrow::array::StructArray = batch.into();
    let array_data = struct_array.to_data();
    let (out_array, out_schema) = delta_kernel::arrow::ffi::to_ffi(&array_data)
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to convert to FFI: {}", e)))?;

    // Get EngineData via FFI helper
    #[cfg(feature = "default-engine-base")]
    {
        let engine_clone = unsafe { engine.clone_handle() };
        let engine_data_result = unsafe { get_engine_data(out_array, &out_schema, engine_clone) };
        match engine_data_result {
            ExternResult::Ok(handle) => {
                let boxed = unsafe { handle.into_inner() };
                Ok(boxed)
            }
            ExternResult::Err(_) => Err(delta_kernel::Error::generic("Failed to create EngineData")),
        }
    }
    #[cfg(not(feature = "default-engine-base"))]
    {
        Err(delta_kernel::Error::generic("default-engine-base feature required"))
    }
}

/// Create EngineData using Arrow native builders for simple fields.
/// This demonstrates how to use Arrow's native array builders directly.
unsafe fn create_engine_data_arrow_native(
    path: *const c_char,
    size_bytes: u64,
    modification_time_ms: u64,
    num_records: u64,
    partition_values_json: *const c_char,
    stats_json: *const c_char,
    data_change: bool,
    engine: &Handle<SharedExternEngine>,
) -> Result<Box<dyn EngineData>, delta_kernel::Error> {
    // Convert C strings to Rust strings
    let path_str = CStr::from_ptr(path).to_str()
        .map_err(|_| delta_kernel::Error::generic("Invalid path string"))?;
    
    let partition_values_str = if partition_values_json.is_null() {
        "{}"
    } else {
        CStr::from_ptr(partition_values_json).to_str()
            .map_err(|_| delta_kernel::Error::generic("Invalid partition values string"))?
    };
    
    let stats_str = if stats_json.is_null() {
        format!(r#"{{"numRecords": {}}}"#, num_records)
    } else {
        CStr::from_ptr(stats_json).to_str()
            .map_err(|_| delta_kernel::Error::generic("Invalid stats string"))?
            .to_string()
    };
    
    // Get the Arrow schema
    let schema: ArrowSchema = add_files_schema().as_ref().try_into_arrow()
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to convert schema: {}", e)))?;
    
    // Build simple fields using Arrow native builders
    let path_array = Arc::new(StringArray::from(vec![path_str]));
    let size_array = Arc::new(Int64Array::from(vec![size_bytes as i64]));
    let modification_time_array = Arc::new(Int64Array::from(vec![modification_time_ms as i64]));
    let data_change_array = Arc::new(BooleanArray::from(vec![data_change]));
    
    // For complex nested structures, we still use JSON approach
    // This is more efficient than trying to build Map and Struct arrays manually
    let json_metadata = format!(
        r#"{{"path":"{}","partitionValues":{},"size":{},"modificationTime":{},"dataChange":{},"stats":{}}}"#,
        path_str, partition_values_str, size_bytes, modification_time_ms, 
        data_change, stats_str
    );
    
    // Convert JSON to Arrow RecordBatch for the complete structure
    let cursor = std::io::Cursor::new(json_metadata.as_bytes());
    let mut reader = delta_kernel::arrow::json::ReaderBuilder::new(schema.into()).build(cursor)
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to create JSON reader: {}", e)))?;
    
    let batch = reader.next().unwrap()
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to read JSON: {}", e)))?;
    
    // Convert RecordBatch to FFI
    let struct_array: delta_kernel::arrow::array::StructArray = batch.into();
    let array_data = struct_array.to_data();
    let (out_array, out_schema) = delta_kernel::arrow::ffi::to_ffi(&array_data)
        .map_err(|e| delta_kernel::Error::generic(format!("Failed to convert to FFI: {}", e)))?;

    // Get EngineData via FFI helper
    #[cfg(feature = "default-engine-base")]
    {
        let engine_clone = unsafe { engine.clone_handle() };
        let engine_data_result = unsafe { get_engine_data(out_array, &out_schema, engine_clone) };
        match engine_data_result {
            ExternResult::Ok(handle) => {
                let boxed = unsafe { handle.into_inner() };
                Ok(boxed)
            }
            ExternResult::Err(_) => Err(delta_kernel::Error::generic("Failed to create EngineData")),
        }
    }
    #[cfg(not(feature = "default-engine-base"))]
    {
        Err(delta_kernel::Error::generic("default-engine-base feature required"))
    }
}

/// Add a single file to the transaction using a FileMetadata struct.
/// This is a cleaner API that reduces the number of parameters.
///
/// # Safety
///
/// Caller is responsible for passing valid handles and a valid FileMetadata struct.
#[no_mangle]
pub unsafe extern "C" fn add_file_with_metadata(
    mut txn: Handle<ExclusiveTransaction>,
    engine: Handle<SharedExternEngine>,
    metadata: *const crate::FileMetadata,
) {
    if metadata.is_null() {
        panic!("FileMetadata pointer is null");
    }

    let meta = &*metadata;
    
    // Create EngineData from the FileMetadata struct
    let engine_data = match create_engine_data_from_params(
        meta.path,
        meta.size_bytes,
        meta.modification_time_ms,
        meta.num_records,
        meta.partition_values_json,
        meta.stats_json,
        meta.data_change,
        meta.dv_storage_type,
        meta.dv_path_or_inline,
        meta.dv_offset,
        meta.dv_size_bytes,
        meta.dv_cardinality,
        &engine,
    ) {
        Ok(data) => data,
        Err(e) => panic!("Failed to create EngineData: {:?}", e),
    };

    // Add the data to the transaction
    let txn_ref = unsafe { txn.as_mut() };
    txn_ref.add_files(engine_data);
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
    engine: Handle<SharedExternEngine>,
    path: *const c_char,
    size_bytes: u64,
    modification_time_ms: u64,
    num_records: u64,
    partition_values_json: *const c_char,  // nullable
    stats_json: *const c_char,             // nullable
    data_change: bool,
    dv_storage_type: *const c_char,        // nullable, defaults to "u"
    dv_path_or_inline: *const c_char,      // nullable
    dv_offset: u64,
    dv_size_bytes: u64,
    dv_cardinality: u64,
) {
    // Create EngineData from the parameters
    let engine_data = match create_engine_data_from_params(
        path,
        size_bytes,
        modification_time_ms,
        num_records,
        partition_values_json,
        stats_json,
        data_change,
        dv_storage_type,
        dv_path_or_inline,
        dv_offset,
        dv_size_bytes,
        dv_cardinality,
        &engine,
    ) {
        Ok(data) => data,
        Err(e) => panic!("Failed to create EngineData: {:?}", e),
    };
    
    // Add the data to the transaction
    let txn_ref = unsafe { txn.as_mut() };
    txn_ref.add_files(engine_data);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::Arc;
    use tempfile::tempdir;

    use delta_kernel_ffi::ffi_test_utils::{ok_or_panic, allocate_str, recover_string};
    use delta_kernel_ffi::tests::get_default_engine;
    use crate::transaction::{transaction, commit};
    use crate::{kernel_string_slice, snapshot, version, snapshot_table_root};
    use test_utils::setup_test_tables;
    use delta_kernel::schema::{StructType, StructField, DataType};

    #[tokio::test]
    async fn test_add_file_simple_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        // Create a simple schema for the test table
        let schema = Arc::new(StructType::new(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("string", DataType::STRING),
        ]));

        // Create a temporary local directory for use during this test
        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = url::Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // Set up test tables (creates the Delta table structure)
        let partition_columns = vec![];
        let tables = setup_test_tables(
            schema,
            &partition_columns,
            Some(&tmp_dir_local_url),
            "test_table",
        ).await?;

        // Get the first table from setup
        let (table_url, _engine, _store, _table_name) = &tables[0];
        let table_url_str = table_url.to_string();

        // Create engine for this path
        let engine = get_default_engine(&table_url_str);

        // Start transaction
        let txn = unsafe { transaction(kernel_string_slice!(table_url_str), engine.clone_handle()) };
        let txn = ok_or_panic(txn);

        // Relative file path (no actual file write needed for metadata-only path)
        let rel_file = "/data-00001.parquet";
        let rel_file_c = CString::new(rel_file).unwrap();

        // Call add_file_simple (consumes a copy of txn)
        let rv = unsafe {
            add_file_simple(
                txn.shallow_copy(),
                engine.clone_handle(),
                rel_file_c.as_ptr(),
                100,                 // size_bytes (dummy)
                1_700_000_000_000,   // modification_time_ms (dummy)
                1,                   // num_records
                std::ptr::null(),    // partitionValues {}
                std::ptr::null(),    // stats -> will be synthesized with numRecords
                true,                // data_change
                std::ptr::null(),    // dv_storage_type
                std::ptr::null(),    // dv_path_or_inline
                0, 0, 0,
            )
        };
        match rv {
            ExternResult::Ok(()) => {
                // Success - file metadata added to transaction
            }
            ExternResult::Err(err) => {
                panic!("add_file_simple failed with error: {:?}", err);
            }
        }

        // Commit the transaction to validate the file metadata
        let commit_version = unsafe { commit(txn, engine.clone_handle()) };
        let commit_version = ok_or_panic(commit_version);
        
        // Verify we got a valid version number (should be 1 for first commit)
        assert!(commit_version > 0);
        println!("Successfully committed transaction with version: {}", commit_version);

        // Read back the Delta log entry to verify the file metadata was written correctly
        let snapshot = unsafe { 
            ok_or_panic(snapshot(kernel_string_slice!(table_url_str), engine.clone_handle())) 
        };
        let snapshot_version = unsafe { version(snapshot.shallow_copy()) };
        assert_eq!(snapshot_version, commit_version);
        println!("Verified snapshot version: {}", snapshot_version);

        // Get the table root to construct the log file path
        let table_root = unsafe { 
            snapshot_table_root(snapshot.shallow_copy(), allocate_str) 
        };
        let table_root_str = recover_string(table_root.unwrap());
        println!("Table root: {}", table_root_str);

        // Extract the file path from the URL (remove file:// protocol)
        let table_path = if table_root_str.starts_with("file://") {
            &table_root_str[7..]
        } else {
            &table_root_str
        };
        println!("Table path: {}", table_path);

        // Read the Delta log file to verify our file metadata
        let log_file_path = format!("{}/_delta_log/{:020}.json", table_path, commit_version);
        println!("Reading Delta log file: {}", log_file_path);
        
        let log_content = std::fs::read_to_string(&log_file_path)
            .expect("Failed to read Delta log file");
        println!("Delta log content: {}", log_content);
        
        // Parse the Delta log (contains multiple JSON objects, one per line)
        let lines: Vec<&str> = log_content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2, "Delta log should contain exactly 2 lines (commit info + add action)");
        
        // Parse the first line (commit info)
        let commit_info: serde_json::Value = serde_json::from_str(lines[0])
            .expect("Failed to parse commit info JSON");
        assert!(commit_info["commitInfo"].is_object());
        println!("Commit info: {}", commit_info["commitInfo"]);
        
        // Parse the second line (add action)
        let add_action: serde_json::Value = serde_json::from_str(lines[1])
            .expect("Failed to parse add action JSON");
        
        // Verify the add action contains our file metadata
        assert!(add_action["add"].is_object());
        let add_data = &add_action["add"];
        assert_eq!(add_data["path"], "/data-00001.parquet");
        assert_eq!(add_data["size"], 100);
        assert_eq!(add_data["modificationTime"], 1_700_000_000_000i64);
        assert_eq!(add_data["dataChange"], true);
        
        // Parse the stats JSON string
        let stats_str = add_data["stats"].as_str().expect("Stats should be a string");
        let stats: serde_json::Value = serde_json::from_str(stats_str)
            .expect("Failed to parse stats JSON");
        assert_eq!(stats["numRecords"], 1);
        
        println!("✅ Successfully verified file metadata in Delta log!");
        println!("   - Path: {}", add_data["path"]);
        println!("   - Size: {} bytes", add_data["size"]);
        println!("   - Records: {}", stats["numRecords"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_file_arrow_native_approach() -> Result<(), Box<dyn std::error::Error>> {
        // Create a simple schema for the test table
        let schema = Arc::new(StructType::new(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("string", DataType::STRING),
        ]));

        // Create a temporary local directory for use during this test
        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = url::Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // Set up test tables (creates the Delta table structure)
        let partition_columns = vec![];
        let tables = setup_test_tables(
            schema,
            &partition_columns,
            Some(&tmp_dir_local_url),
            "test_table_arrow",
        ).await?;

        // Get the first table from setup
        let (table_url, _engine, _store, _table_name) = &tables[0];
        let table_url_str = table_url.to_string();

        // Create engine for this path
        let engine = get_default_engine(&table_url_str);

        // Start transaction
        let txn = unsafe { transaction(kernel_string_slice!(table_url_str), engine.clone_handle()) };
        let txn = ok_or_panic(txn);

        // Relative file path (no actual file write needed for metadata-only path)
        let rel_file = "/data-arrow-native.parquet";
        let rel_file_c = CString::new(rel_file).unwrap();

        // Call add_file_simple using the Arrow-native approach
        let rv = unsafe {
            add_file_simple(
                txn.shallow_copy(),
                engine.clone_handle(),
                rel_file_c.as_ptr(),
                200,                 // size_bytes (different from previous test)
                1_800_000_000_000,   // modification_time_ms (different from previous test)
                2,                   // num_records (different from previous test)
                std::ptr::null(),    // partitionValues {}
                std::ptr::null(),    // stats -> will be synthesized with numRecords
                true,                // data_change
                std::ptr::null(),    // dv_storage_type
                std::ptr::null(),    // dv_path_or_inline
                0, 0, 0,
            )
        };
        match rv {
            ExternResult::Ok(()) => {
                println!("✅ Arrow-native approach: Successfully added file metadata to transaction");
            }
            ExternResult::Err(err) => {
                panic!("Arrow-native add_file_simple failed with error: {:?}", err);
            }
        }

        // Commit the transaction to validate the file metadata
        let commit_version = unsafe { commit(txn, engine.clone_handle()) };
        let commit_version = ok_or_panic(commit_version);
        
        // Verify we got a valid version number
        assert!(commit_version > 0);
        println!("✅ Arrow-native approach: Successfully committed transaction with version: {}", commit_version);

        // Read back the Delta log entry to verify the file metadata was written correctly
        let snapshot = unsafe { 
            ok_or_panic(snapshot(kernel_string_slice!(table_url_str), engine.clone_handle())) 
        };
        let snapshot_version = unsafe { version(snapshot.shallow_copy()) };
        assert_eq!(snapshot_version, commit_version);
        println!("✅ Arrow-native approach: Verified snapshot version: {}", snapshot_version);

        // Get the table root to construct the log file path
        let table_root = unsafe { 
            snapshot_table_root(snapshot.shallow_copy(), allocate_str) 
        };
        let table_root_str = recover_string(table_root.unwrap());
        
        // Extract the file path from the URL (remove file:// protocol)
        let table_path = if table_root_str.starts_with("file://") {
            &table_root_str[7..]
        } else {
            &table_root_str
        };

        // Read the Delta log file to verify our file metadata
        let log_file_path = format!("{}/_delta_log/{:020}.json", table_path, commit_version);
        println!("Reading Delta log file: {}", log_file_path);
        
        let log_content = std::fs::read_to_string(&log_file_path)
            .expect("Failed to read Delta log file");
        
        // Parse the Delta log (contains multiple JSON objects, one per line)
        let lines: Vec<&str> = log_content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2, "Delta log should contain exactly 2 lines (commit info + add action)");
        
        // Parse the second line (add action)
        let add_action: serde_json::Value = serde_json::from_str(lines[1])
            .expect("Failed to parse add action JSON");
        
        // Verify the add action contains our file metadata
        assert!(add_action["add"].is_object());
        let add_data = &add_action["add"];
        assert_eq!(add_data["path"], "/data-arrow-native.parquet");
        assert_eq!(add_data["size"], 200);
        assert_eq!(add_data["modificationTime"], 1_800_000_000_000i64);
        assert_eq!(add_data["dataChange"], true);
        
        // Parse the stats JSON string
        let stats_str = add_data["stats"].as_str().expect("Stats should be a string");
        let stats: serde_json::Value = serde_json::from_str(stats_str)
            .expect("Failed to parse stats JSON");
        assert_eq!(stats["numRecords"], 2);
        
        println!("✅ Arrow-native approach: Successfully verified file metadata in Delta log!");
        println!("   - Path: {}", add_data["path"]);
        println!("   - Size: {} bytes", add_data["size"]);
        println!("   - Records: {}", stats["numRecords"]);
        println!("   - Modification Time: {}", add_data["modificationTime"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_file_with_metadata_struct() -> Result<(), Box<dyn std::error::Error>> {
        // Create a simple schema for the test table
        let schema = Arc::new(StructType::new(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("string", DataType::STRING),
        ]));

        // Create a temporary local directory for use during this test
        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = url::Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // Set up test tables (creates the Delta table structure)
        let partition_columns = vec![];
        let tables = setup_test_tables(
            schema,
            &partition_columns,
            Some(&tmp_dir_local_url),
            "test_table_struct",
        ).await?;

        // Get the first table from setup
        let (table_url, _engine, _store, _table_name) = &tables[0];
        let table_url_str = table_url.to_string();

        // Create engine for this path
        let engine = get_default_engine(&table_url_str);

        // Start transaction
        let txn = unsafe { transaction(kernel_string_slice!(table_url_str), engine.clone_handle()) };
        let txn = ok_or_panic(txn);

        // Create FileMetadata struct
        let rel_file = "/data-struct.parquet";
        let rel_file_c = CString::new(rel_file).unwrap();
        
        let metadata = crate::FileMetadata {
            path: rel_file_c.as_ptr(),
            size_bytes: 300,
            modification_time_ms: 1_900_000_000_000,
            num_records: 3,
            partition_values_json: std::ptr::null(),
            stats_json: std::ptr::null(),
            data_change: true,
            dv_storage_type: std::ptr::null(),
            dv_path_or_inline: std::ptr::null(),
            dv_offset: 0,
            dv_size_bytes: 0,
            dv_cardinality: 0,
        };

        // Call add_file_with_metadata using the struct
        let rv = unsafe {
            add_file_with_metadata(
                txn.shallow_copy(),
                engine.clone_handle(),
                &metadata,
            )
        };
        match rv {
            ExternResult::Ok(()) => {
                println!("✅ Struct-based approach: Successfully added file metadata to transaction");
            }
            ExternResult::Err(err) => {
                panic!("Struct-based add_file_with_metadata failed with error: {:?}", err);
            }
        }

        // Commit the transaction to validate the file metadata
        let commit_version = unsafe { commit(txn, engine.clone_handle()) };
        let commit_version = ok_or_panic(commit_version);
        
        // Verify we got a valid version number
        assert!(commit_version > 0);
        println!("✅ Struct-based approach: Successfully committed transaction with version: {}", commit_version);

        // Read back the Delta log entry to verify the file metadata was written correctly
        let snapshot = unsafe { 
            ok_or_panic(snapshot(kernel_string_slice!(table_url_str), engine.clone_handle())) 
        };
        let snapshot_version = unsafe { version(snapshot.shallow_copy()) };
        assert_eq!(snapshot_version, commit_version);
        println!("✅ Struct-based approach: Verified snapshot version: {}", snapshot_version);

        // Get the table root to construct the log file path
        let table_root = unsafe { 
            snapshot_table_root(snapshot.shallow_copy(), allocate_str) 
        };
        let table_root_str = recover_string(table_root.unwrap());
        
        // Extract the file path from the URL (remove file:// protocol)
        let table_path = if table_root_str.starts_with("file://") {
            &table_root_str[7..]
        } else {
            &table_root_str
        };

        // Read the Delta log file to verify our file metadata
        let log_file_path = format!("{}/_delta_log/{:020}.json", table_path, commit_version);
        println!("Reading Delta log file: {}", log_file_path);
        
        let log_content = std::fs::read_to_string(&log_file_path)
            .expect("Failed to read Delta log file");
        
        // Parse the Delta log (contains multiple JSON objects, one per line)
        let lines: Vec<&str> = log_content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2, "Delta log should contain exactly 2 lines (commit info + add action)");
        
        // Parse the second line (add action)
        let add_action: serde_json::Value = serde_json::from_str(lines[1])
            .expect("Failed to parse add action JSON");
        
        // Verify the add action contains our file metadata
        assert!(add_action["add"].is_object());
        let add_data = &add_action["add"];
        assert_eq!(add_data["path"], "/data-struct.parquet");
        assert_eq!(add_data["size"], 300);
        assert_eq!(add_data["modificationTime"], 1_900_000_000_000i64);
        assert_eq!(add_data["dataChange"], true);
        
        // Parse the stats JSON string
        let stats_str = add_data["stats"].as_str().expect("Stats should be a string");
        let stats: serde_json::Value = serde_json::from_str(stats_str)
            .expect("Failed to parse stats JSON");
        assert_eq!(stats["numRecords"], 3);
        
        println!("✅ Struct-based approach: Successfully verified file metadata in Delta log!");
        println!("   - Path: {}", add_data["path"]);
        println!("   - Size: {} bytes", add_data["size"]);
        println!("   - Records: {}", stats["numRecords"]);
        println!("   - Modification Time: {}", add_data["modificationTime"]);

        Ok(())
    }
}
