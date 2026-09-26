"""Regenerate sample.parquet. Run: python3 build.py (requires pyarrow)."""
import pyarrow as pa, pyarrow.parquet as pq
from datetime import datetime, timezone
from decimal import Decimal

rows = []
for i in range(5):
    rows.append({
        "vnum": 100 + i,
        "vdec": Decimal("1.23456789") + Decimal(i),
        "vdate": datetime(2025, 1, 1, 12, 0, i, tzinfo=timezone.utc),
        "vb": (i % 2 == 0),
        "vreal": 3.14 + i,
        "vstr": f"parquet-row-{i}",
        "vlong": 9000000000 + i,
        "vtype": "bytes-parquet",
    })
schema = pa.schema([
    ("vnum", pa.int32()),
    ("vdec", pa.decimal128(18, 8)),
    ("vdate", pa.timestamp("us", tz="UTC")),
    ("vb", pa.bool_()),
    ("vreal", pa.float64()),
    ("vstr", pa.string()),
    ("vlong", pa.int64()),
    ("vtype", pa.string()),
])
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "sample.parquet", compression="snappy")
