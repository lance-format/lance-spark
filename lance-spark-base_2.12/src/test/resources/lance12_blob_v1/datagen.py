"""
Writes the Lance 12 tables that BaseBlobCreateTableTest reads.

Lance 12's fragment writer stored legacy (v1) blob fields in 2.2 files without converting them, so
each table holds the v1 layout in a 2.2 data file next to a 2.1 one. Lance 13 turns such a field
into blob v2 on the next commit that keeps that file, and the new version cannot be read.

Run with pylance 12.0.0; it rewrites both tables next to this script.
"""

import shutil
from pathlib import Path

import lance
import pyarrow as pa
from lance.fragment import LanceFragment

EXPECTED_LANCE_VERSION = "12.0.0"

assert lance.__version__ == EXPECTED_LANCE_VERSION

BLOB_V1 = {"lance-encoding:blob": "true"}


def write(name, schema, rows, appended_rows):
    path = Path(__file__).parent / name
    shutil.rmtree(path, ignore_errors=True)
    lance.write_dataset(
        pa.Table.from_pylist(rows, schema), path, data_storage_version="2.1"
    )
    fragment = LanceFragment.create(
        path, pa.Table.from_pylist(appended_rows, schema), data_storage_version="2.2"
    )
    return lance.LanceDataset.commit(
        path, lance.LanceOperation.Append([fragment]), read_version=1
    )


top_level = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("data", pa.large_binary(), metadata=BLOB_V1),
    ]
)
write(
    "top_level.lance",
    top_level,
    [{"id": 1, "data": b"one"}, {"id": 2, "data": b"two"}],
    [{"id": 3, "data": b"three"}],
).create_scalar_index("id", "BTREE", name="id_idx")

nested = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=False),
        pa.field(
            "payload",
            pa.struct([pa.field("data", pa.large_binary(), metadata=BLOB_V1)]),
        ),
    ]
)
write(
    "nested.lance",
    nested,
    [{"id": 1, "payload": {"data": b"one"}}],
    [{"id": 2, "payload": {"data": b"two"}}],
)
