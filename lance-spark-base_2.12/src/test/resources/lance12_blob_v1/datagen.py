"""
Writes the Lance 12 tables that BaseBlobCreateTableTest reads.

Lance 12's fragment writer stored legacy (v1) blob fields in 2.2 files without converting them, so
each table holds the v1 layout in a 2.2 data file next to a 2.1 one. Lance 13 turns such a field
into blob v2 on the next commit that keeps that file, and the new version cannot be read.

Run with pylance 12.0.0; it rewrites both tables next to this script.

spark_append_22.lance is not written here: lance-spark 0.9.0-beta.2, on Lance 12, created it at
2.1 with a v1 blob column and appended two rows to it with file_format_version 2.2, so the table
reports 2.1 while its only data files are 2.2 ones.
"""

import shutil
from pathlib import Path

import lance
import pyarrow as pa
from lance.fragment import LanceFragment

EXPECTED_LANCE_VERSION = "12.0.0"

assert lance.__version__ == EXPECTED_LANCE_VERSION

BLOB_V1 = {"lance-encoding:blob": "true"}


def write(name, schema, rows, appended_rows, index=None):
    path = Path(__file__).parent / name
    shutil.rmtree(path, ignore_errors=True)
    dataset = lance.write_dataset(
        pa.Table.from_pylist(rows, schema), path, data_storage_version="2.1"
    )
    if index is not None:
        # Indexed before the append, so REFRESH INDEX has the 2.2 fragment left to cover.
        dataset.create_scalar_index(index, "BTREE", name=index + "_idx")
    fragment = LanceFragment.create(
        path, pa.Table.from_pylist(appended_rows, schema), data_storage_version="2.2"
    )
    lance.LanceDataset.commit(
        path,
        lance.LanceOperation.Append([fragment]),
        read_version=lance.dataset(path).version,
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
    index="id",
)

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
