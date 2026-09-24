<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# PyPI benchmark dataset

The benchmarks use an existing copy of the public PyPI download dataset in S3.
Normal benchmark runs start with the [README preparation steps](README.md#disposable-ec2-environment); exporting the source again is unnecessary.
This document records the source, its Athena schema, the original preparation procedure, and the smaller tables created for measurements.

## Recorded source

The source is the daily partition `bigquery-public-data.pypi.file_downloads$20260917` of the [public PyPI download dataset](https://console.cloud.google.com/marketplace/product/gcp-public-data-pypi/pypi).
It was exported on 2026-09-19 with a BigQuery extract job and copied to S3 without rewriting the Parquet files.

| Property | Recorded value |
|----------|----------------|
| Download date | 2026-09-17 (UTC) |
| Athena catalog and table | `AwsDataCatalog.pyathena_benchmark.pypi_file_downloads` |
| Hive partition | `download_date='2026-09-17'` (`string`) |
| S3 table root | `s3://pyathena-benchmark/pypi_file_downloads/` |
| S3 partition | `s3://pyathena-benchmark/pypi_file_downloads/download_date=2026-09-17/` |
| Rows | 4,773,620,317 |
| Parquet objects | 6,800 |
| Total object bytes | 32,591,505,117 (approximately 30.4 GiB) |
| Compression | Snappy |
| Athena region / workgroup | `us-west-2` / `pyathena` |
| Minimum `timestamp` | `1789603200000000` (2026-09-17 00:00:00 UTC) |
| Maximum `timestamp` | `1789689599000000` (2026-09-17 23:59:59 UTC) |

The row count and timestamp bounds come from the original Athena validation.
The Glue table, partition definition, object count, and total object bytes were checked again on 2026-09-19 while writing this document, without scanning the dataset.
The byte total describes compressed source objects, not decoded memory usage or the size of Athena query results.
A new export can produce different files, ordering, or data if the upstream partition changes; preserve this S3 copy for comparisons.

## Source table DDL

The following DDL matches the existing Glue column types and locations.
Run each statement separately in Athena when registering an equivalent dataset in a new environment.
The existing source is already registered; the benchmark program does not execute this DDL.
When using a different bucket or database, change both locations and the identifiers, then update `config.toml` and the CloudFormation source parameters.
The database/table creation and partition registration create catalog metadata only; they do not export or copy data.

```sql
CREATE DATABASE IF NOT EXISTS pyathena_benchmark;

CREATE EXTERNAL TABLE pyathena_benchmark.pypi_file_downloads (
  `timestamp` bigint,
  `country_code` string,
  `url` string,
  `project` string,
  `file` struct<
    `filename`:string,
    `project`:string,
    `version`:string,
    `type`:string
  >,
  `details` struct<
    `installer`:struct<`name`:string,`version`:string,`subcommand`:string>,
    `python`:string,
    `implementation`:struct<`name`:string,`version`:string>,
    `distro`:struct<
      `name`:string,
      `version`:string,
      `id`:string,
      `libc`:struct<`lib`:string,`version`:string>
    >,
    `system`:struct<`name`:string,`release`:string>,
    `cpu`:string,
    `openssl_version`:string,
    `setuptools_version`:string,
    `rustc_version`:string,
    `ci`:boolean
  >,
  `tls_protocol` string,
  `tls_cipher` string,
  `http` struct<
    `method`:string,
    `status_code`:bigint,
    `bytes_served`:bigint,
    `range_header`:string
  >
)
PARTITIONED BY (`download_date` string)
STORED AS PARQUET
LOCATION 's3://pyathena-benchmark/pypi_file_downloads/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

ALTER TABLE pyathena_benchmark.pypi_file_downloads
ADD PARTITION (download_date='2026-09-17')
LOCATION 's3://pyathena-benchmark/pypi_file_downloads/download_date=2026-09-17/';
```

The DDL above includes every nested field.
The main groups are:

| Column | Athena type | Contents |
|--------|-------------|----------|
| `timestamp` | `bigint` | Download time represented as Unix epoch microseconds |
| `country_code`, `url`, `project` | `string` | Country code, requested URL, and package project |
| `file` | `struct` | File name, project, version, and distribution type |
| `details` | `struct` | Installer, Python implementation, operating system, and client environment details |
| `tls_protocol`, `tls_cipher` | `string` | TLS connection details |
| `http` | `struct` | Method, status code, bytes served, and range header |
| `download_date` | `string` partition key | Date supplied by the Hive partition, absent from the exported Parquet columns |

The exported probe file stores `timestamp` as physical Parquet `INT64` with a microsecond timestamp annotation (`isAdjustedToUTC=false`).
The registered Athena table deliberately exposes its integer values as `bigint`; the original probe validation checked these values against the exported file.
The benchmark projections preserve this integer type, so this workload does not benchmark native timestamp conversion.
For a human-readable UTC display, use `from_unixtime(CAST("timestamp" AS double) / 1000000, 'UTC')`; retain the integer column when exact microsecond values matter.
Nested fields can be null and remain nested in the exported files.

## Recreating the source copy

Source preparation is separate from the disposable benchmark CloudFormation stack.
It requires a Google Cloud project with BigQuery job permissions, access to the public table, a compatible Cloud Storage staging bucket, and permission to populate an S3 source bucket and register its Glue table.
The stack's EC2 role grants read-only access to the source and cannot perform this preparation.

### Export the daily partition

Create a private Cloud Storage staging bucket in the source dataset's `US` location and select a fresh export prefix.
The following command expresses the extract configuration used for the recorded copy; replace the project and bucket placeholders before running it.
The single quotes around the source table preserve the literal `$` partition decorator.
See the [BigQuery export instructions](https://docs.cloud.google.com/bigquery/docs/exporting-data) for permissions and destination-location requirements.

```bash
bq --project_id=YOUR_EXPORT_PROJECT --location=US extract \
  --destination_format=PARQUET \
  --compression=SNAPPY \
  'bigquery-public-data:pypi.file_downloads$20260917' \
  'gs://YOUR_STAGING_BUCKET/exports/UNIQUE_RUN/pypi_file_downloads/download_date=2026-09-17/part-*.parquet'
```

Wait for the extract job to succeed before transferring files.
Record the job configuration, source schema, row count, and source modification time before and after the export.
The original preparation checked that the source schema, row count, and modification time did not change during extraction.
Record every exported object's relative name, byte size, generation, and available checksums in an inventory outside the Parquet data prefix.

### Copy the files to S3

The recorded copy used an AWS DataSync Enhanced mode task with an agentless Google Cloud Storage source and an S3 destination.
Follow the [DataSync Google Cloud Storage setup](https://docs.aws.amazon.com/datasync/latest/userguide/tutorial_transfer-google-cloud-storage.html) to create the temporary credentials, locations, and task.
Use these settings to reproduce the transfer:

| Setting | Value |
|---------|-------|
| Source server | `storage.googleapis.com`, HTTPS, port 443 |
| Source bucket and subdirectory | Staging bucket and the exact export prefix above |
| Destination | A fresh S3 prefix ending in `pypi_file_downloads/download_date=2026-09-17/` |
| Task mode | `ENHANCED` |
| `VerifyMode` | `ONLY_FILES_TRANSFERRED` |
| `TransferMode` | `CHANGED` |
| `OverwriteMode` | `ALWAYS`; start with an empty destination prefix |
| `PreserveDeletedFiles` | `PRESERVE` |
| `ObjectTags` | `NONE` |

Grant the temporary Google service account object-read access to the staging bucket, and scope the DataSync S3 role to the destination and transfer-report prefixes.
Transfer one file to a separate probe prefix first, inspect its Parquet schema and timestamp values through Athena, then transfer the complete export to the empty final partition prefix.
Keep reports, inventories, and probe data outside the final Parquet partition.
Wait for successful transfer verification and compare the complete relative-name/size inventories at both ends.
The original destination used private S3 storage with SSE-S3 encryption.

Register the table and partition with the DDL above, then run the validation below.
After successful validation, remove the temporary DataSync tasks and locations, transfer-only IAM resources, Google HMAC keys and service account, staging objects, and probe objects/table.
The original preparation left an empty private GCS staging bucket; delete a newly created staging bucket too when it is no longer needed.
Retain the S3 source files, table, partition, and preparation evidence for subsequent benchmark runs.

## Validation and preparation evidence

Run these metadata checks from `benchmarks/` using the [local `.env` configuration](README.md#dependencies-and-local-checks).
They do not run Athena queries:

```bash
uv run --env-file ../.env --locked aws glue get-table \
  --database-name pyathena_benchmark --name pypi_file_downloads
uv run --env-file ../.env --locked aws glue get-partition \
  --database-name pyathena_benchmark --table-name pypi_file_downloads \
  --partition-values 2026-09-17
uv run --env-file ../.env --locked aws s3api list-objects-v2 \
  --bucket pyathena-benchmark \
  --prefix 'pypi_file_downloads/download_date=2026-09-17/' \
  --query '{objects:length(Contents),bytes:sum(Contents[].Size)}' --output json
```

For the recorded copy, expect 6,800 objects and 32,591,505,117 bytes.
Matching these totals alone does not prove that two copies contain identical data; retain the complete inventories and transfer-verification results.

The following Athena validation scans the selected source partition and is separate from benchmark timing:

```sql
SELECT
  count(*) AS row_count,
  min("timestamp") AS min_timestamp_us,
  max("timestamp") AS max_timestamp_us
FROM pyathena_benchmark.pypi_file_downloads
WHERE download_date = '2026-09-17';
```

Its recorded result is the row count and timestamp bounds in the first table.
The original preparation also validated scalar/nested queries, partition pruning, and an UNLOAD whose 15 Parquet files contained 1,000 rows in total.
Preparation evidence is retained under `s3://pyathena-benchmark/manifests/pypi_file_downloads/2026-09-17/20260919T014535Z/` for identities with access to that prefix.
It includes source metadata, export/transfer inventories, validation results, and the preparation summary; it is not part of the benchmark stack's source-prefix grant.

## Tables used for measurements

`prepare` creates one fixed table per requested scale in the stack's scratch database.
The default scales in [config.toml](config.toml) are 10,000, 100,000, 1,000,000, and 10,000,000 rows.
For example, the small-scale CTAS has this form, with placeholders supplied by the program:

```sql
CREATE TABLE "<scratch_database>"."b_<run_id>_small"
WITH (
  format='PARQUET',
  write_compression='SNAPPY',
  external_location='s3://<scratch_bucket>/runs/<run_id>/inputs/small/'
) AS
SELECT *
FROM "pyathena_benchmark"."pypi_file_downloads"
WHERE download_date = '2026-09-17'
LIMIT 10000;
```

These snapshots are not partitioned; the selected `download_date` becomes an ordinary string column.
`prepare` checks the actual row count and writes the executed SQL, Glue column schema, table name, S3 location, and query IDs to the input manifest.
The source schema in this document describes the exported files; the manifest describes the CTAS output actually used for a run.
Selection has no `ORDER BY`, so a new preparation need not choose the same rows.
Reuse a prepared manifest and its tables when comparing revisions.

The flat projection selects 15 scalar columns, including selected fields from `file`, `details`, and `http`.
The nested projection selects nine top-level columns and preserves all three structures.
Both omit `download_date` from the measured result and read the same prepared table without a further `LIMIT`; their definitions are in [config.py](pyathena_bench/config.py).
Full cleanup removes these scratch tables and their data, while `cleanup --trials-only` preserves them for another run.
Neither mode deletes the original source table or its S3 partition.
