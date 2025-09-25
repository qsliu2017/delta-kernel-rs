# Attaching a Deletion Vector (DV) - Example

This example demonstrates the intended FFI workflow to attach a deletion vector to an
existing Parquet file in a Delta table. The sequence is:

- Start a transaction on the table.
- Stage a `remove` action describing the parquet file with the *old* DV.
- Stage an `add` action for the same parquet file with the *new* DV attached.
- Commit the transaction (the commit JSON will contain `remove` then `add`).

Files:
- `attach_dv.c` — C example showing the sequence using the existing FFI helpers.
- `CMakeLists.txt` — Build file for the example.

Notes:
- The repo's FFI currently exposes `add_file_with_metadata` which creates `add` actions.
  There is not yet a dedicated `add_remove` helper; this example shows how the intended
  workflow should look once a `remove`-staging API exists on the FFI side.
- After implementing `remove` staging and ensuring the transaction writes `remove` actions,
  a commit produced by this example should contain consecutive `remove` and `add` log
  actions for the same parquet file, with respective `deletionVector` objects.

