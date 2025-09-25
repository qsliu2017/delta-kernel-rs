#include <stdio.h>
#include <string.h>
#include "delta_kernel_ffi.h"

// This example demonstrates the intended FFI usage to attach a deletion vector (DV)
// to an existing parquet file by staging a remove (old DV) and add (new DV) in one
// transaction and committing.

// TODO: Implement this
static struct EngineError *allocate_error(enum KernelError etype, struct KernelStringSlice msg) {
    // Allocate a minimal EngineError and print the message for debugging.
    struct EngineError *err = (struct EngineError *)malloc(sizeof(struct EngineError));
    if (!err) {
        return NULL;
    }
    err->etype = etype;
    // Print the provided message (not required, but useful for examples).
    if (msg.ptr && msg.len > 0) {
        fprintf(stderr, "Engine error (%d): %.*s\n", (int)etype, (int)msg.len, msg.ptr);
    } else {
        fprintf(stderr, "Engine error: %d\n", (int)etype);
    }
    return err;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <table_path>\n", argv[0]);
        return 2;
    }

    const char *table_path = argv[1];

    // 1. Get an engine for the table (helper from read_table example)
    // Build or get a default engine. The generated header requires an allocate_error
    // function pointer; for examples we pass NULL which some builds accept.
    KernelStringSlice table_path_slice = { table_path, strlen(table_path) };
#if defined(DEFINE_DEFAULT_ENGINE_BASE)
    ExternResultHandleSharedExternEngine engine_res = get_default_engine(table_path_slice, allocate_error);

    // If transaction creation fails because the table does not exist, we could create
    // the table here. The FFI currently does not expose a dedicated "create table"
    // helper; callers should create the table ahead of time. For the example we
    // simply proceed only if the transaction was created successfully.

    if (engine_res.tag != OkHandleSharedExternEngine) {
        fprintf(stderr, "Failed to get default engine\n");
        return 1;
    }
    HandleSharedExternEngine engine = engine_res.ok;

    // 2. Start a transaction on the table
    ExternResultHandleExclusiveTransaction txn_res = transaction(table_path_slice, engine);
    if (txn_res.tag != OkHandleExclusiveTransaction) {
        fprintf(stderr, "Failed to start transaction\n");
        return 1;
    }
    HandleExclusiveTransaction txn = txn_res.ok;
#else
    fprintf(stderr, "Default engine base feature not available in this build\n");
    return 1;
#endif


// (Optional) Example: stage a simple add_file to the transaction. This is not strictly
// necessary for DV attach, but shows how to use `add_file_with_metadata` for plain adds.
// Here we create a small placeholder add (commented out) – uncomment and adjust if needed.
//
// FileMetadata init_add = {0};
// init_add.path = "/initial_dummy.parquet";
// init_add.size_bytes = 1;
// init_add.modification_time_ms = 0;
// init_add.num_records = 0;
// init_add.data_change = true;
// add_file_with_metadata(txn, engine, &init_add);


    // 3. Stage a remove action for the existing parquet file with the OLD DV descriptor
    FileMetadata remove_meta = {0};
    remove_meta.path = "/part-00001.parquet"; // relative path
    remove_meta.size_bytes = 0; // unknown
    remove_meta.modification_time_ms = 0;
    remove_meta.num_records = 0;
    remove_meta.partition_values_json = NULL;
    remove_meta.stats_json = NULL;
    remove_meta.data_change = true;
    // deletion vector fields to describe the OLD DV
    remove_meta.dv_storage_type = "u"; // unique id style
    remove_meta.dv_path_or_inline = "OLDPREFIXabcde..."; // base85 uuid-like
    remove_meta.dv_offset = 0;
    remove_meta.dv_size_bytes = 1234;
    remove_meta.dv_cardinality = 42;

    // Use the FFI: add_file_with_metadata expects (txn_handle, engine_handle, metadata)
    add_file_with_metadata(txn, engine, &remove_meta);

    // 4. Stage an add action for the same parquet file with the NEW DV descriptor
    FileMetadata add_meta = {0};
    add_meta.path = "/part-00001.parquet";
    add_meta.size_bytes = 1000; // size of the parquet file
    add_meta.modification_time_ms = 1700000000000;
    add_meta.num_records = 100;
    add_meta.partition_values_json = NULL;
    add_meta.stats_json = NULL;
    add_meta.data_change = false; // attaching a DV is not a data change
    add_meta.dv_storage_type = "u";
    add_meta.dv_path_or_inline = "NEWPREFIXvwxyz...";
    add_meta.dv_offset = 0;
    add_meta.dv_size_bytes = 2345;
    add_meta.dv_cardinality = 100;

    add_file_with_metadata(txn, engine, &add_meta);

    // 5. Commit the transaction
    ExternResultu64 commit_res = commit(txn, engine);
    if (commit_res.tag != Oku64) {
        fprintf(stderr, "Commit failed\n");
    } else {
        printf("Committed version: %llu\n", (unsigned long long)commit_res.ok);
    }

    // 6. Free handles
    free_transaction(txn);
    free_engine(engine);

    return 0;
}
