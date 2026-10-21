# Write parquet that Power BI likes

Power BI **Direct Lake** reads a Delta table's parquet directly: the first query transcodes each
parquet **row group into one VertiPaq segment**, and every query after scans those segments. So
query time, and the capacity units you pay, are a property of how the parquet was written.

This repo measures that on real Fabric capacity: five datasets, four writers (delta-rs, DuckDB,
Fabric Warehouse, Fabric Spark) producing the **same rows**, one semantic model and one DAX suite
over each.

**We use only real data, never synthetic.**

- **Results:** <https://djouallah.github.io/direct-lake-parquet-layout/>
- **How it works** (parquet anatomy, transcoding, why sorting matters):
  <https://djouallah.github.io/direct-lake-parquet-layout/how.html>
- **The rules and the numbers behind them:** [docs/FINDINGS.md](docs/FINDINGS.md)
- **Run it:** [docs/RUNNING.md](docs/RUNNING.md) · **CI:** [docs/CI.md](docs/CI.md)
