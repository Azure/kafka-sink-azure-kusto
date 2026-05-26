# Binary format test fixtures

Pre-built sample files used by `KustoSinkIT` to exercise ingestion of binary
columnar formats whose rows cannot be assembled record-by-record by the
connector (the connector only ships pre-serialized payloads through Kafka's
byte-passthrough writer).

## Files

| File | Rows | Schema | Purpose |
|------|------|--------|---------|
| `sample.parquet` | 5 | `vnum:int, vdec:decimal(18,8), vdate:timestamp, vb:bool, vreal:double, vstr:string, vlong:long, vtype:string` | Regression coverage for #174 — verifies parquet ingestion works end-to-end after the DataFormat dispatch fix. |

## Regeneration

Edit `build.py` and run with `python3 build.py`. Requires `pyarrow`.

`vlong` values are unique per row and used as the assertion key in
`KustoSinkIT.performDataAssertions`. `vtype` is fixed to `bytes-parquet` so
the assertion query can filter the rows produced by this test in isolation.
