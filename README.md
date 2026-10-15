# DataValidations V4 — offline HTML dashboard, benchmarks, and Spark integration tests

This upgrades the **screenshot-reconstructed** PySpark regression validator, **not a byte-for-byte copy of the original work script**. File1 = Bigplay and File2 = DaaS by default.

## What's new in V4

- **Self-contained dashboard:** `DataValidation_dashboard.html` appears in each run folder. It embeds CSS and JavaScript; opening the file requires **no server, internet connection, CDN, browser extensions, external libraries, or dashboard hosting**. The report includes status, row/key metrics, mismatch ranking, searchable columns, schema findings, validation reasons, stage timings, and up to a configurable number of example mismatch rows. Selecting a column filters the displayed samples; all underlying discrepancies remain in the CSV reports.
- **Stage timings** are saved in `DataValidation_summary.json`, including loading, key identification, joins, missing-row reporting, output writing, reconciliation, and HTML writing. They are elapsed times, **not Spark executor CPU times**.
- **Benchmark harness:** `benchmark_data_validation_v4.py` produces reproducible synthetic source/target CSVs entirely in Spark, executes repeated validations, and writes median/min/max wall times and per-run stage times to `benchmark_results.json` and `benchmark_results.csv`. It excludes input-generation time from validation time, but separately records generation time.
- **Expanded regression tests:** 25 unit/integration tests including first-valid-key early exit, composite key collisions, nulls, numeric tolerance, schema changes, missing keys in both directions, conversion detection, financial reconciliation, dashboard security, PASS/REVIEW/FAIL, and end-to-end report generation.
- **V3 reports and options remain supported.** The per-column outputs, wide report, missing-records report, summary, schema/quality reports, configurable comparison tolerances, and financial reconciliation continue unchanged. Run outputs now live in `DataValidation_v4/<run_id>/` so prior results aren't overwritten.

## Primary key behavior — intentionally retained

When no explicit key is provided, the script **checks File1 columns in File1 order**. For each column it checks `countDistinct` against that file's row count; only if File1 qualifies does it check File2. **It stops immediately at the first column unique and non-null in both datasets**. It does not review later columns. This optimizes your preferred strategy; it is **not** guaranteed to identify the business key. Pass `--composite ACCOUNT_ID+FUND_ID` to override when needed; explicit key components are checked for NULLs and duplicates in both files.

## Requirements

- Python 3.9+ and a supported **PySpark / Spark** runtime compatible with your Java version.
- Spark file read/write permission to the configured input/output locations.
- JSON support is built in. Optional `PyYAML` only if you use YAML configuration.
- The HTML generator uses Python standard library only; no `pandas`, `plotly`, `jinja2`, or web server is needed.
- For cloud or cluster execution, use HDFS/object-store locations accessible to **all Spark executors**. `file://` may only work for local mode or truly shared filesystem mounts.

## Quick start

Keep `data_Validation_comp_v4.py` and `data_validation_dashboard.py` together.

```bash
spark-submit --master 'local[2]' \
  data_Validation_comp_v4.py \
  --bigplay_file /data/bigplay.csv \
  --daas_file /data/daas.csv \
  --output_dir file:///tmp/dv-results \
  --run_id demo1
```

For an **immediate synthetic smoke test**, run from the extracted project folder:

```bash
spark-submit --master 'local[2]' data_Validation_comp_v4.py \
  --config examples/quickstart_config.json
```

The example source/target CSVs are fabricated; the output is written beneath `/tmp/data_validation_demo/DataValidation_v4/quickstart/`. It contains mismatched values and missing keys, but no real customer data.

Using a configuration (edit paths and columns in `validation_v4_config.example.json` first):

```bash
spark-submit --master 'local[2]' data_Validation_comp_v4.py \
  --config validation_v4_config.example.json
```

For cluster submission, explicitly ship the helper module to the driver as needed:

```bash
spark-submit --master yarn --deploy-mode client \
  --py-files data_validation_dashboard.py \
  data_Validation_comp_v4.py --config validation_v4_config.example.json
```

Set `dashboard.enabled: false` to disable the HTML. `dashboard.sample_rows: 0` disables example values while retaining summary metrics. `dashboard.max_columns: 500` caps how many ranked column rows appear **in HTML only**; CSV data is never capped by this option. The default example limit is **100 discrepant column/PK rows**, across all affected columns (Spark `limit` is not a random, balanced sample). It is bounded to protect driver memory and limit HTML size. All mismatches are still written to the partitioned CSV outputs.

## Output locations

For `--output_dir file:///tmp/dv-results --run_id demo1`, files appear in:

`/tmp/dv-results/DataValidation_v4/demo1/`

| Artifact | Description |
| --- | --- |
| `DataValidation_dashboard.html` | Portable HTML dashboard, generated after reconciliation; contains bounded **actual values**, so handle as sensitive |
| `DataValidation_summary.json` | Counts, statuses, configuration context, and stage timings |
| `DataValidation_differencesall_records.csv/` | Wide record-level discrepancies, original paired source/target values |
| `DataValidation_differences_by_column/For_column=.../` | Per-column discrepancy partition folders with `PK, Column_affected, File1, File2` followed by difference metadata |
| `DataValidation_column_summary.csv/` | Column-level mismatch ranking/counts/rates |
| `DataValidation_top_affected_keys.csv/` | Most affected keys when differences are present and top-key reporting enabled |
| `DataValidation_differencesmissing_records.csv/` | Missing keys in either input |
| `DataValidation_schema_differences.csv/` | Schema mismatches |
| `DataValidation_invalid_conversions.csv/` | Numeric/date/explicit-cast errors |
| `DataValidation_data_quality.csv/` | Required-field NULLs |
| `DataValidation_aggregate_reconciliation.csv/` | Optional grouped sums and PASS/FAIL per group/metric |

**Important:** Spark CSV output paths ending in `.csv` are normally **directories** holding one or more `part-*.csv` files. Local browser hyperlinks in the dashboard may not open remote HDFS/S3 paths. Use a secure filesystem browser there.

## Open the HTML without Spark

A **demonstration dashboard** is included as `demo_dashboard.html` using entirely artificial values. To regenerate a dashboard from an existing locally accessible JSON summary, without rerunning Spark:

```bash
python data_validation_dashboard.py \
  --summary /path/to/DataValidation_summary.json \
  --output /path/to/new_dashboard.html
```

That command renders aggregates, schema, and timing metadata but won't contain example mismatches unless you also supply `--samples samples.json` containing an array of discrepancy objects. The V4 Spark run normally embeds a small number of example rows directly during processing.

## How to benchmark

First, run with small numbers to verify file access and Spark resources:

```bash
spark-submit --master 'local[2]' benchmark_data_validation_v4.py \
  --work-dir file:///tmp/dv4-bench \
  --sizes 1000 10000 \
  --columns 10 30 \
  --repetitions 2 \
  --warmups 1 \
  --partitions 2 \
  --metrics-dir ./benchmark_results
```

On an adequately sized cluster, you can repeat at `--sizes 100000 1000000 10000000`, but **start smaller** and monitor storage, shuffles, executor memory, and output count. Data generation itself can be costly. For a fair comparison of revisions, keep cluster size, Spark version, file storage, `--columns`, input schemas, partition counts, caching, tolerances, warmups, and dashboard options constant. Compare the **median** of multiple runs, not a single fastest run. Benchmark input generation is outside the reported `validation_seconds` but actual CSV output, discrepancy generation, dashboard, and summary writes are part of validation. The reported `records` count is **per input file**.

Metrics:
- `benchmark_results.json`: full run metadata (Spark version/master), per-run timings, and median/min/max by input size/width.
- `benchmark_results.csv`: flat per-run results for Excel/Power BI.

`--no-dashboard` enables an apples-to-apples performance test of dashboard overhead; note this is then a different end-to-end workload and should be labeled accordingly. Benchmark code intentionally **does not** claim that the early-exit PK heuristic is a business key.

The example dataset is generated with deterministic differences: some File2 keys are changed, a small fraction of numeric values differ, and a smaller fraction of status values differ. `ID` is first and unique so the key search should stop immediately.

## Tests

Syntax/unit tests even **without** Spark:

```bash
python -m compileall -q .
python -m unittest discover -s tests -v
```

In a Spark/PySpark environment, the same test command also executes **15 Spark integration cases**, including full runs and actual output reads. Run these before production use:

```bash
PYTHONPATH=. python -m unittest discover -s tests -v
```

Integration tests use a local `SparkSession`, temporary CSV files, and actual Spark joins/writes. Don't claim these tests passed until they've run in your environment.

## Known limitations, security and reliability

- **Full Spark integration and benchmarks have NOT been run in the authoring environment.** PySpark is not installed there. Syntax and 10 no-Spark unit tests passed; all 15 integration tests were correctly skipped. Reported benchmark results are generated only when you run the benchmark in your Spark environment.
- The automatically selected key may differ from your intended business key. This is your intentional design preference.
- All output CSV directories and the HTML may contain account identifiers or confidential financial values; **never publish a real-run dashboard or outputs in a public GitHub repository**. The demo HTML uses fabricated data only. Follow company security and code ownership policies for sharing the script itself.
- Input `infer_schema: true` can strip leading zeros from account numbers/CUSIPs. Prefer `infer_schema: false` and explicitly configure numeric columns for finance/insurance data.
- The dashboard samples the first N rows returned by Spark `limit`, so it may omit some affected columns entirely. Use full CSVs for investigation; do **not** interpret the dashboard examples as representative sampling.
- Spark's stage timings here are **instrumented driver elapsed times**, not Spark UI stage-level breakdowns. Use Spark UI/history server for executor/shuffle diagnosis.
- Performance varies with physical partitioning, skew, data source, cache warmth, columns, and cluster configuration. No guaranteed speedup is claimed.
- Core SQL validation rules, Spark DataFrame operations, and infrastructure integrations need review against your organization's Spark version and standards before production use.
