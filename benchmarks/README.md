<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Cursor benchmarks

This project measures result retrieval, memory use, and concurrent query behavior for PyAthena and AWS SDK for pandas (AWS Wrangler), and S3 transfers through PyAthena's filesystems.
It includes a disposable CloudFormation environment and does not run benchmarks in CI.
Recorded measurements and the recommendations drawn from them are listed in [RESULTS.md](RESULTS.md).

## Input data

The supplied input is `pyathena_benchmark.pypi_file_downloads`, with the Hive partition `download_date='2026-09-17'` (UTC).
The source contains 4,773,620,317 rows, approximately 30.4 GiB of Parquet compressed with Snappy.
The default region and existing Athena workgroup are `us-west-2` and `pyathena`.
See [the dataset reference](DATASET.md) for the full source DDL, nested column definitions, timestamp representation, export/transfer procedure, validation results, and measurement-table layout.

`prepare` creates one fixed Parquet/Snappy snapshot per selected scale in the stack's scratch database and bucket.
Defaults are 10,000, 100,000, 1,000,000, and 10,000,000 rows.
Every adapter reads the same snapshot without another `LIMIT`.
Preparation verifies the actual row count and records SQL, schema, table names, query IDs, and locations in a manifest.
Selection from the source uses `LIMIT` without ordering: separate preparations can select different rows.
Preserve a snapshot for comparisons across revisions, and use a separate output directory for each run.
The Athena timings describe queries against these smaller snapshots, not a scan of the complete source partition.

The flat workload projects scalar fields from `file`, `details`, and `http` alongside the top-level scalar columns.
The nested workload preserves those structures.
The json workload adds `CAST(details AS JSON) AS details_json` to the flat projection; it covers native pandas and Wrangler reads in the single-query suite, with their UNLOAD and CTAS reads marked unsupported, because Parquet cannot hold the `json` type.
The temporal workload adds columns derived from `"timestamp"` with `from_unixtime()` to the flat projection: `event_time` (`timestamp(3)`), `event_date` (`date`), `event_time_utc`, `event_time_zone`, and `event_time_offset` (`timestamp(3) with time zone` in `UTC`, `Asia/Tokyo`, and `+09:00`), and `event_clock` (`time(3)`).
It covers every family in the single-query suite; its UNLOAD and CTAS reads add only `event_time` and `event_date`, because UNLOAD and CTAS cannot write `time` or `with time zone` types.
No workload converts every library's output into a common Python object representation.

## Dependencies and local checks

The benchmark is a non-packaged member of the repository's uv workspace and runs on Python 3.12.
It shares the root `uv.lock` with PyAthena, which it uses from the workspace checkout.
Locally and in CI, its virtual environment is `benchmarks/.venv`, separate from the root `.venv`.
On the EC2 instance, the dedicated checkout's root `/opt/pyathena/.venv` holds the benchmark environment.
The `polars1` and `polars2` dependency groups select Polars 1.x or 2.x, which PolarsCursor reads with different code paths.
The environments above have `polars1`; a second environment beside each, `benchmarks/.venv-polars2` or `/opt/pyathena/.venv-polars2`, has `polars2`.
The root `pyproject.toml` declares both groups and the root `dev` group as conflicting, so `uv.lock` resolves each of them separately.
The root `uv build -v` still builds only PyAthena, including a wheel built from its sdist.
Benchmark dependencies do not become PyAthena runtime dependencies, and `uv sync --group dev` at the root does not install them.

From the repository root:

```bash
just benchmark lint
just benchmark test
```

The recipes select `benchmarks/.venv` and Python 3.12, and sync that environment from the locked workspace; `just benchmark test` also runs the tests in `benchmarks/.venv-polars2`.
The tests use local data, fake AWS clients, and child processes.
They do not execute Athena queries or create AWS resources.
`just benchmark format` formats the benchmark separately from the parent project.
After changing dependencies, extras, or dependency groups in either `pyproject.toml`, run `uv lock` at the repository root and commit `uv.lock`.

Commands below run from `benchmarks/`.
Set the environment variables first; without them, uv uses the root `.venv`.
`plan` and `report` do not contact AWS or require `.env`.

```bash
cd benchmarks
export UV_PROJECT_ENVIRONMENT="$PWD/.venv" UV_PYTHON=3.12
uv run --locked python -m pyathena_bench plan --suite single --scale small
```

To run a command with Polars 2.x, select the second environment and its group:

```bash
UV_PROJECT_ENVIRONMENT="$PWD/.venv-polars2" uv run --locked --no-group polars1 --group polars2 \
  python -m pyathena_bench plan --suite single --scale small --family polars
```

For local AWS access, configure the profile and region in the repository root's gitignored `.env` file:

```dotenv
AWS_PROFILE=your-aws-profile
AWS_DEFAULT_REGION=us-west-2
```

Use `KEY=value` assignments and prefix local AWS commands with `uv run --env-file ../.env` from `benchmarks/`.
In a git worktree, run `just worktree-env` from the repository root once to link its `.env`.
Leave the benchmark's `--profile` option and `[aws].profile` setting unset to use `AWS_PROFILE`.
The harness still reads its region and workgroup from `config.toml`; keep these aligned with the target stack.
The EC2 instructions in [Disposable EC2 environment](#disposable-ec2-environment) use the instance role without loading the local `.env`.

Review the case count before running a suite.
`run` requires explicit suite and scale selections and never overwrites an existing output directory.
Use `--family`, `--api`, `--transport`, and `--output-kind` to narrow both `plan` and `run`.
Copy `config.toml` to an ignored location to customize scales, repetitions, timeouts, memory limits, or executor sizes, and pass it with `--config` before the subcommand.
Keep the same configuration for preparation, measurement, and cleanup.

## Disposable EC2 environment

Deploy `cloudformation/benchmark.yaml` in `us-west-2` with the stack tag `Purpose=pyathena-benchmark`.
The template creates its own VPC, public subnet, route, Internet Gateway, IAM role, scratch Glue database, scratch S3 bucket, and an Auto Scaling group of `FleetSize` identical EC2 instances with their EBS volumes.
`FleetSize` defaults to one host; see [Running on a fleet](#running-on-a-fleet) for parallel runs.
There is no inbound security-group rule or SSH key; use Session Manager.
Internet access permits package installation and calls to AWS APIs without a NAT gateway.

The defaults are Amazon Linux 2023 x86_64, `r7i.2xlarge` (8 vCPUs, 64 GiB), and an encrypted 100 GiB gp3 root volume.
Set `Architecture=arm64` with an `r7g`, `r8g`, or `c8gn.2xlarge` instance type to run on AWS Graviton; the template rejects an instance type that does not match the architecture.
Measurements depend on the CPU architecture; `environment.json` records it, and runs on different architectures are not directly comparable.
Instance size and disk size are parameters, so a later run can deliberately test another memory budget.
The hosts are Spot Instances of one instance type by default; `Purchase=on-demand` launches On-Demand Instances of that type instead.
The Auto Scaling group spreads the hosts over `Zones` Availability Zones (4 by default, or 3) with the price-capacity-optimized allocation strategy and never mixes instance types, so every host stays identical.
The template uses the first `Zones` zones that `Fn::GetAZs` returns, which depends on the zones in which the account's default VPC has default subnets; `us-west-2`, the default region, has four, and the fourth widens Spot capacity, which was missing in two of three zones in the 4.0.0 run.
Where `Fn::GetAZs` returns three zones, deploy with `Zones=3`; with the default, the deployment fails with a stack error.
The group replaces an interrupted Spot Instance; with `QueueName` set, the replacement's worker continues the interrupted job, see [Running on a fleet](#running-on-a-fleet).
`environment.json` does not record the instance type or purchase option; a worker started with `--terminate-host` records both under `hosts/` in its queue, and the recovered `instances.json` lists the hosts that still exist.
The AMI parameter resolves the current AL2023 image at deployment; record the resulting AMI when comparing environments.
Source bucket access is read-only and restricted to the configured prefix.
The instances can write only to the scratch bucket and database and to the results location below, and can query the specified existing workgroup.
The supplied template assumes ordinary IAM access and SSE-S3 source objects; Lake Formation restrictions or a customer-managed KMS key require corresponding grants before execution.

The fleet queues, reports, and Athena trial outputs go to the stack's results location, the `Results` stack output.
With `ResultsBucket` set to an existing bucket, the results location is `s3://<ResultsBucket>/<ResultsPrefix>/<stack name>/`, and the instance role can read and write objects only below it, while it can list the whole bucket, because S3 answers a read of a missing key with 403 instead of 404 without that permission; with `ResultsBucket` empty, the default, it is the scratch bucket.
Input snapshots, initialization fixtures, and scratch tables stay in the scratch bucket and database either way.
This repository's CI test bucket, which `AWS_ATHENA_S3_STAGING_DIR` names in CI, expires every object one day after it is written, at the following midnight UTC (`cloudformation/github_actions_oidc.yaml`).
As the results location, it removes leftovers by itself, but reports must be recovered within that day.

The deployment identity needs permission to create these resources and pass the instance role.
Use an immutable, remotely accessible commit that contains this directory and the root `uv.lock`.
Bootstrap checks out that commit, installs pinned uv and Python versions, and syncs the benchmark with `uv sync --locked`: with the `polars1` group into `/opt/pyathena/.venv`, and with the `polars2` group into `/opt/pyathena/.venv-polars2`.
It signals setup completion to CloudFormation but does not prepare data or start measurements.
The EC2 commands use `--no-sync` to reuse this verified environment; they do not revalidate the lockfile on every invocation.

From the local checkout's `benchmarks/` directory, replace the commit below with the implementation commit:

```bash
BENCHMARK_STACK=pyathena-benchmark-run
BENCHMARK_COMMIT="<full-40-character-commit-sha>"
uv run --env-file ../.env --locked aws cloudformation deploy \
  --stack-name "$BENCHMARK_STACK" \
  --template-file cloudformation/benchmark.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Purpose=pyathena-benchmark \
  --parameter-overrides GitCommit="$BENCHMARK_COMMIT"
uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK"
BENCHMARK_GROUP=$(uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`AutoScalingGroup`].OutputValue | [0]' --output text)
BENCHMARK_INSTANCE=$(uv run --env-file ../.env --locked aws autoscaling describe-auto-scaling-groups \
  --auto-scaling-group-names "$BENCHMARK_GROUP" --query 'AutoScalingGroups[0].Instances[0].InstanceId' --output text)
uv run --env-file ../.env --locked aws ssm start-session --target "$BENCHMARK_INSTANCE"
```

Session Manager requires the AWS CLI Session Manager plugin on the local machine.
Inside the session, switch to `ec2-user` and enter the benchmark directory:

```bash
sudo -iu ec2-user
cd /opt/pyathena/benchmarks
export AWS_DEFAULT_REGION=us-west-2
BENCHMARK_STACK_ID=$(cat stack-id.txt)
uv run --no-sync python -m pyathena_bench preflight --stack "$BENCHMARK_STACK_ID"
uv run --no-sync python -m pyathena_bench prepare \
  --stack "$BENCHMARK_STACK_ID" --manifest results/input.json --scale small
```

`preflight` only reads metadata.
It checks the live stack identity, source location and partition, and whether the workgroup permits a dedicated S3 output location.
`prepare` submits CTAS and count queries and incurs Athena and S3 usage.
Select additional scales in the initial preparation command when needed.
Each preparation needs a new manifest filename; failed preparation leaves a manifest for cleanup.

## Running the suites

Start with a small, selected single-query comparison:

```bash
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/single-small \
  --suite single --scale small --family cursor --transport csv
```

Remove the family filter to include all families, or compare native DataFrame output:

```bash
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/native-small \
  --suite single --scale small --family pandas arrow polars wrangler --output-kind native
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/nested-small \
  --suite single --scale small --shape nested --output-kind native
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/temporal-small \
  --suite single --scale small --shape temporal
```

The concurrency suite compares ThreadPool and native asyncio APIs at 1, 10, 50, and 100 simultaneous queries by default.
It uses a bounded matrix without the chunk-size sweep.
Use the small snapshot first; requested concurrency does not override the account's Athena quotas.
The default workgroup is also used by this repository's integration CI.
Schedule measurements while CI and other Athena workloads in the account are idle; a separate workgroup alone does not isolate [account-wide quotas](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html).
An alternative existing workgroup can be selected with the CloudFormation `WorkGroup` parameter and matching configuration, without changing CI.
Throttling, queuing, and failed queries remain visible in the output.
Both asynchronous integrations offload blocking operations when required: ThreadPool `execute()` and result consumption use an asyncio executor, and native DataFrame accessors on Aio cursors also need offloading.
The configured asyncio executor has 32 workers by default; the shared AsyncCursor owns a separate pool of the same size.
The SDK connection pool accommodates both executors and the requested concurrency.
Native libraries and S3 readers can create additional threads.

```bash
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/concurrent-small \
  --suite concurrent --scale small
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/init-small \
  --suite init --scale small
```

The initialization suite prepares completed CSV and UNLOAD query results once, outside the timed trials.
It compares direct synchronous ResultSet construction with `asyncio.to_thread()` construction using the same stored output.
Construction can include S3 I/O and conversion; the difference is not a measurement of pure scheduler overhead.
It validates the full row count after construction, outside `init_seconds`.

### Filesystem suite

The filesystem suite transfers one S3 object through PyAthena's `S3FileSystem` (API `sync`) and `AioS3FileSystem` (API `aio`) and does not query Athena.
Its operations, selected with `--transport`, are:

- `read`: one `read()` of the whole object from a file opened with `block_size`, which reads it as parallel range requests of that size and holds the parts and the joined object, about twice the object's size, in memory;
- `write`: writes through a file opened with `block_size`, which uploads the object as a multipart upload with parts of that size;
- `copy`: `cp_file()`, or `_cp_file()` of `AioS3FileSystem`, which copies an object over 5 GiB as a multipart copy with parts of `block_size`; the copies of smaller objects, single CopyObject requests, are recorded as unsupported; the copies exclude tags and annotations, which the generated objects do not have.

Each case sets the filesystem's `max_workers` and the S3 client's `max_pool_connections` through the connection's `s3_config`.
The `[filesystem]` section of `config.toml` sets the swept values, 8 and 40 workers and pools of 10 and 40 by default, and a `block_size` of 8 MiB.
The suite's scales are the object sizes in bytes in `[filesystem] sizes`, not the row counts in `[scales]`: 64 MiB (small), 256 MiB (medium), 1 GiB (large), and 6 GiB (xlarge) by default.

The `sync` API calls the filesystem on the trial's thread, and a file's parallel requests run in an `S3ThreadPoolExecutor`.
The `aio` API runs a read or write in `asyncio.to_thread()`, as `AioS3FSCursor` reads result files, and a file's parallel requests run through `S3AioExecutor`, dispatched on the filesystem's event loop, fsspec's IO thread, to a pool of `max_workers` threads of the file.
Its copy is a coroutine awaited on the trial's event loop, whose part copies run in the filesystem's pool of `max_workers` threads.
Both event loops keep Python's default executor, which runs only the trial's `asyncio.to_thread()` call.
Before #1159, the parallel requests of the `aio` API ran in the default executors, whose `min(32, os.cpu_count() + 4)` threads bound them.

The source objects, of zero bytes, are uploaded with boto3 below the run's scratch prefix and recorded under `objects` in the manifest.
`prepare --filesystem-scale` uploads them with the manifest, so that every fleet host reads the same objects.
Otherwise, `run` uploads the missing objects before its first trial, and a fleet worker uploads them on its host, to its own copy of the manifest.
Writes and copies go to `trials/<trial>/object` below the scratch prefix.
`total_seconds` and the event-loop lag cover the transfer; afterwards, the trial checks the size of the data read or of the written or copied object, and deletes the written or copied object.
`cleanup` removes the source objects and any remaining trial objects, the latter also with `--trials-only`.
The summary shows the throughput in MiB per second and the peak thread count.
A manifest for this suite alone needs no snapshot: without `--scale`, `prepare` creates none.

```bash
uv run --no-sync python -m pyathena_bench prepare \
  --stack "$BENCHMARK_STACK_ID" --manifest results/input-filesystem.json --filesystem-scale large
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input-filesystem.json --out results/filesystem-large \
  --suite filesystem --scale large
```

Reads and writes are bounded by the host's sustained network bandwidth: 3.75 Gbps on `r7g.2xlarge` and 25 Gbps on `c8gn.2xlarge`, both with 8 vCPUs, so that the default executor of an event loop has 12 threads on either.
Copies are not: their UploadPartCopy requests name the source range, and S3 copies the bytes.
`c8gn.2xlarge` has 16 GiB of memory, in which the 6 GiB read, holding about twice the object, leaves no margin below the default RSS limit; on it, select reads and writes up to large, where copies are unsupported, and writes and copies at xlarge:

```bash
uv run --no-sync python -m pyathena_bench jobs \
  --suite filesystem --scale small medium large --transport read write > results/jobs-filesystem.json
uv run --no-sync python -m pyathena_bench jobs \
  --suite filesystem --scale xlarge --transport write copy > results/jobs-filesystem-xlarge.json
```

`jobs` reads the scales of the filesystem suite from `[filesystem] sizes`; a `jobs` command that also selects another suite accepts only scale names defined in both tables, so expand the filesystem suite in its own command when its scales differ and combine the job files with `queue --jobs`.

## Measurement semantics

Each warmup and measured trial runs in a fresh child process, with imports outside the measurement baseline.
All DataFrame backends are imported before the baseline, including for row cursors.
Absolute RSS therefore includes the harness's common imports; inspect the baseline and increase when interpreting memory overhead.
A concurrent batch shares one child process, so its RSS belongs to the batch rather than an individual query.
Defaults are one discarded warmup and five measured trials per case, executed serially by the parent.
Warmups can affect service-side caches, but do not warm the next child's in-process caches.
No attempt is made to flush operating-system or Athena caches.
Athena result reuse and library query caches are disabled.

| Measurement | Meaning |
| --- | --- |
| `setup_seconds` | Connection and cursor preparation |
| `execute_seconds` | Public execute call through completion, including waiting for a Future or coroutine |
| `consume_seconds` | Consumption after execute completes |
| `total_seconds` | Execute plus full result consumption; excludes connection preparation and cleanup; the transfer in the filesystem suite |
| `post_completion_setup_seconds` | First observed successful Athena status through result readiness, when available |
| `init_seconds` | ResultSet construction in the initialization suite |
| Athena statistics | Server execution, queue time, scanned bytes, and engine version from SDK responses |
| RSS samples | Externally sampled resident memory, including setup and validation; 50 ms by default |
| Process high-water RSS | OS high-water value, including imports and process startup |
| Event-loop lag | Delay beyond the heartbeat interval; 10 ms interval by default |
| Thread metrics | Observed process threads and CPU time since the baseline, including native library threads |
| Peak temp dir | Largest sampled size of the trial's Polars temporary directory |

Some cursors eagerly read data during `execute()`, while others fetch lazily.
Consequently, `consume_seconds` alone is not a fair retrieval comparison.
Compare end-to-end times for the same output representation, then inspect server-side statistics separately.
The one-second polling interval can dominate small-result timings; keep it identical across cases and inspect client result time separately.
The Markdown summary includes client result time from observed completion through consumption, initialization time, and median RSS increase.
Client result time excludes the delay between server completion and its observation, so it is not a pure local CPU measurement or a replacement for end-to-end time.
API row conversion and native DataFrame/Table access appear as separate cases.
Sampled RSS can miss short peaks, and the constructor suite's RSS includes its subsequent validation read.
If the operating system denies access to per-thread CPU times, `thread_cpu_available` is false; thread counts and RSS are still recorded.
Each trial process receives its own `POLARS_TEMP_DIR`, which the parent samples with RSS and removes after the trial.
Polars before 1.39.0 downloaded the whole S3 object of a lazy CSV scan into a file cache in this directory before yielding batches and left the file after the process exited; PyAthena requires Polars 1.39.0 or later, whose scans do not write that cache.
When the temporary directory is a tmpfs, as `/tmp` is on Amazon Linux 2023, files written there use memory that RSS does not include.

| Capability | Treatment |
| --- | --- |
| Cursor, DictCursor, S3FS | Row batches with arraysize 100 and 1000 |
| Pandas CSV | Full DataFrame or streamed chunks |
| Pandas UNLOAD | Full DataFrame or chunks, each also with `dtype_backend="pyarrow"` |
| Arrow CSV and UNLOAD | Eager table loading; arraysize changes row extraction, not S3 streaming |
| Polars CSV and UNLOAD | Full result or chunk iterator |
| SQLAlchemy (`sqla_*`) | `pandas.read_sql_query()` through an engine of each PyAthena driver, whole or with `chunksize` |
| Wrangler CSV | Scalar projections, including the json and temporal workloads; nested cases are marked unsupported |
| Wrangler CTAS and UNLOAD | Full DataFrame or chunks, including nested workloads |
| S3 filesystem (`s3`) | Read, write, and multipart copy of one object with `S3FileSystem` and `AioS3FileSystem` |

The SQLAlchemy families `sqla_rest`, `sqla_pandas`, `sqla_arrow`, `sqla_polars`, and `sqla_s3fs` read with the synchronous API in the single-query suite only, and the result-file drivers also read UNLOAD results.
Their engines connect with the same workgroup, output location, polling interval, and disabled result reuse as the cursor cases, their statements run without the client cache (`cache_size` defaults to 0), and their cursors take the same cursor options; with `chunksize`, the pandas and Polars drivers' cursors also read the result file in chunks.
As a cursor is, the engine's connection is opened during setup, outside `total_seconds`.
`pandas.read_sql_query()` builds DataFrames from the rows of `fetchall()` or `fetchmany()`, so these cases include the driver's row output; `sqla_rest` pages through GetQueryResults like Cursor.

The [Wrangler API documentation](https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.athena.read_sql_query.html) describes its CSV nested-type restriction.
CTAS and UNLOAD have separate limitations, including `time` and timestamp-with-time-zone columns, which the temporal workload leaves out of its CTAS and UNLOAD reads.
Unsupported combinations are included in the plan and report with reasons.
An attempted operation that fails is recorded as a failure, not silently reclassified as unsupported.
Row-count validation detects incomplete consumption; it is not a proof that different libraries produce identical dtypes or nested Python objects.

The parent stops a trial at the configured timeout or RSS fraction of physical RAM and attempts to cancel observed active queries.
The suite stops at the first failed trial, including a failed warmup, so outstanding queries cannot affect later measurements.
Inspect the recorded failure and quiesce the trial outputs with `cleanup --trials-only --execute` before retrying selected cases in a new output directory against the same manifest.
This preserves the input snapshots and initialization fixtures, so comparisons still use identical rows.
Both cleanup modes cancel and wait for all active queries belonging to the run, including orphaned preparation or fixture queries; `--trials-only` narrows deletion, not cancellation.
Retain the previous local reports before cleaning remote trial outputs.
An external kill is reported as a worker exit, not automatically as an out-of-memory error.
Use `cleanup` after interrupted runs to discover outstanding queries whose IDs were not returned before a worker died.
For example, after stopping the failed process:

```bash
uv run --no-sync python -m pyathena_bench cleanup --manifest results/input.json --trials-only
uv run --no-sync python -m pyathena_bench cleanup --manifest results/input.json --trials-only --execute
uv run --no-sync python -m pyathena_bench run \
  --manifest results/input.json --out results/retry-small \
  --suite single --scale small --family pandas --api thread --output-kind native
```

There is no automatic continuation of a partially recorded trial; rerun the selected comparison with the same manifest.
`run --resume` names the output directories of earlier attempts and skips the measured trials that they recorded as successful, after running its own warmups, which a fleet worker uses to continue an interrupted job.
A fleet worker also passes `--checkpoint`, with which the run writes each trial's start and record to S3 between trials and writes no summary of its own; the job's summary merges its attempts.
Large API row cases may exceed the default one-hour timeout: 10 million rows require at least 10,000 pages with arraysize 1000, or 100,000 pages with arraysize 100.
Choose the timeout using a smaller-scale pilot before the full run.
Do not run two orchestrators concurrently on the same dedicated host.

## Running on a fleet

Large scales and API row retrieval can take hours per case, so a single host cannot cover a full large-scale matrix in a working session.
Deploy with `FleetSize` greater than one to run independent jobs on identical hosts.
All hosts share the stack's scratch bucket and database: prepare once, and every host measures the same snapshot through the same manifest.
Each host runs one worker, and each worker runs one orchestrator at a time, so trials on a host never overlap.

Athena executes queries from different hosts independently, but API request rates are account-wide.
Cursor and DictCursor row retrieval calls GetQueryResults for every page, including the initialization suite's row-count validation, about 5 to 10 pages per second per query in the recorded runs; check the account's GetQueryResults rate in Service Quotas (100 calls per second in the tested account).
`jobs` marks these jobs as API-heavy with an estimated page count and the number of simultaneous paging queries, which is the concurrency level for the concurrent suite.
`worker --api-slots` bounds the simultaneous paging queries across the fleet (10 by default): a job takes one slot per paging query, and a worker refuses to start if any job needs more slots than that.
While an API-heavy job waits for slots, workers do not start later API-heavy jobs; other jobs run on every free host.

`jobs` splits a selection into one `run` invocation per suite, scale, shape, family, API, transport, and output kind, and per arraysize for row output.
It orders API-heavy jobs first and longer jobs earlier.
With `--split-pages`, a job whose single trial needs at least that many pages becomes one job per measured repetition without a warmup, so those repetitions run on different hosts.
`run` accepts the matching `--arraysize`, `--warmups`, and `--repetitions` options.
`--chunksize` keeps only the given chunk sizes, with 0 for reads without chunks, in `plan`, `run`, and `jobs`; `jobs` passes it to each job and adds it to the job ID.
`jobs` accepts `--warmups` and `--repetitions` too; with either option, every job that is not split carries both values as arguments, so one queue can combine jobs with different trial counts under one stored configuration.
`queue --jobs` accepts several job files, rejects duplicate job IDs, and orders the combined jobs as `jobs` orders one selection.
`jobs --environment polars2` runs the selected jobs with Polars 2.x: each job names the environment, and its ID ends with `-polars2`, so one queue can hold the same cases for both Polars versions.
A worker runs such a job with the interpreter of `.venv-polars2` beside its own environment, and refuses to start if a job's environment is missing on its host.
Select the families that use Polars, `polars` and `sqla_polars`, for this environment.

Deploy a new stack with the fleet size under its own name; changing `FleetSize` on an existing stack does not wait for the added hosts to finish bootstrap.
`QueueName` names the queue that every host consumes: after bootstrap, each host starts a worker with the stack's results location from its user data, also written to `results-location.txt` next to `stack-id.txt`, and the worker waits until a queue of that name is published; this includes the hosts that the group launches later to replace interrupted Spot Instances.
`ApiSlots` sets their `--api-slots`.
`ResultsBucket` selects the results location; the commands below use this repository's CI test bucket, whose objects expire after a day.
Then open a session on one of its hosts:

```bash
BENCHMARK_STACK=pyathena-benchmark-fleet
BENCHMARK_RESULTS_BUCKET="<ci-test-bucket>"
uv run --env-file ../.env --locked aws cloudformation deploy \
  --stack-name "$BENCHMARK_STACK" \
  --template-file cloudformation/benchmark.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Purpose=pyathena-benchmark \
  --parameter-overrides GitCommit="$BENCHMARK_COMMIT" FleetSize=12 Architecture=arm64 InstanceType=r7g.2xlarge \
  ResultsBucket="$BENCHMARK_RESULTS_BUCKET" QueueName=large-1 ApiSlots=10
BENCHMARK_GROUP=$(uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`AutoScalingGroup`].OutputValue | [0]' --output text)
BENCHMARK_INSTANCE=$(uv run --env-file ../.env --locked aws autoscaling describe-auto-scaling-groups \
  --auto-scaling-group-names "$BENCHMARK_GROUP" --query 'AutoScalingGroups[0].Instances[0].InstanceId' --output text)
uv run --env-file ../.env --locked aws ssm start-session --target "$BENCHMARK_INSTANCE"
```

Prepare and publish the queue in that session:

```bash
sudo -iu ec2-user
cd /opt/pyathena/benchmarks
export AWS_DEFAULT_REGION=us-west-2
BENCHMARK_STACK_ID=$(cat stack-id.txt)
mkdir -p results
cp config.toml results/fleet.toml  # edit scales, timeouts, and concurrency as needed
uv run --no-sync python -m pyathena_bench --config results/fleet.toml prepare \
  --stack "$BENCHMARK_STACK_ID" --manifest results/input-large.json --scale large xlarge
uv run --no-sync python -m pyathena_bench --config results/fleet.toml jobs \
  --suite single init --scale large xlarge --shape flat nested --split-pages 20000 > results/jobs-large.json
uv run --no-sync python -m pyathena_bench --config results/fleet.toml queue \
  --stack "$BENCHMARK_STACK_ID" --name large-1 --jobs results/jobs-large.json --manifest results/input-large.json
```

Several `jobs` selections can make up one bounded matrix.
For example, the following selection, with `chunksizes = [10000, 100000, 1000000]` and `timeout_seconds = 5400.0` in `results/fleet.toml`, measures the synchronous API only:

- medium as a small regression subset: Cursor and S3FS rows with arraysize 1000, and the native reads of the DataFrame families and Wrangler without chunks, flat, three repetitions;
- large, flat and nested: Cursor and S3FS rows with arraysize 1000 (five repetitions), Cursor with arraysize 100 (three, split into single-repetition jobs), DictCursor flat (three), every native read of the DataFrame families and Wrangler (five), the json workload's CSV reads without chunks and with 100,000-row chunks (five), and `read_sql_query()` through the pandas, Arrow, Polars, and S3FS drivers without chunks and with 100,000-row chunks (five) and through the REST driver without chunks (three, split); `sqla_s3fs` covers the chunked `fetchmany()` path without GetQueryResults paging;
- xlarge: S3FS rows flat, every native read of the DataFrame families and Wrangler, the json workload without chunks and with 100,000-row chunks, and `read_sql_query()` through the pandas, Arrow, Polars, and S3FS drivers flat without chunks and with chunks, and through the pandas, Arrow, and Polars drivers nested with chunks only, all with three repetitions.

It leaves out the ThreadPool and native asyncio APIs, whose single-query timings matched the synchronous API, the initialization suite, the concurrency suite, arraysize 100 for every family but Cursor, and DictCursor beyond one scale.
Every selection runs without warmups because each trial already runs in a fresh process.

```bash
jobs() { uv run --no-sync python -m pyathena_bench --config results/fleet.toml jobs --warmups 0 "$@"; }
jobs --suite single --scale medium --shape flat --api sync --arraysize 1000 --chunksize 0 \
  --family cursor s3fs --output-kind rows --repetitions 3 > results/jobs-medium-rows.json
jobs --suite single --scale medium --shape flat --api sync --chunksize 0 \
  --family pandas arrow polars wrangler --output-kind native --repetitions 3 > results/jobs-medium-native.json
jobs --suite single --scale large --shape flat nested --api sync --arraysize 1000 \
  --family cursor s3fs --output-kind rows --repetitions 5 > results/jobs-large-rows.json
jobs --suite single --scale large --shape flat nested --api sync --arraysize 100 \
  --family cursor --repetitions 3 --split-pages 5000 > results/jobs-large-cursor-100.json
jobs --suite single --scale large --shape flat --api sync --arraysize 1000 \
  --family dict --repetitions 3 > results/jobs-large-dict.json
jobs --suite single --scale large --shape flat nested --api sync \
  --family pandas arrow polars wrangler --output-kind native --repetitions 5 > results/jobs-large-native.json
jobs --suite single --scale large --shape json --api sync --transport csv --chunksize 0 100000 \
  --family pandas wrangler --repetitions 5 > results/jobs-large-json.json
jobs --suite single --scale large --shape flat nested --chunksize 0 100000 \
  --family sqla_pandas sqla_arrow sqla_polars sqla_s3fs --repetitions 5 > results/jobs-large-sqla.json
jobs --suite single --scale large --shape flat nested --chunksize 0 \
  --family sqla_rest --repetitions 3 --split-pages 1000 > results/jobs-large-sqla-rest.json
jobs --suite single --scale xlarge --shape flat --api sync --arraysize 1000 \
  --family s3fs --output-kind rows --repetitions 3 > results/jobs-xlarge-s3fs.json
jobs --suite single --scale xlarge --shape flat nested --api sync \
  --family pandas arrow polars wrangler --output-kind native --repetitions 3 > results/jobs-xlarge-native.json
jobs --suite single --scale xlarge --shape json --api sync --transport csv --chunksize 0 100000 \
  --family pandas wrangler --repetitions 3 > results/jobs-xlarge-json.json
jobs --suite single --scale xlarge --shape flat --chunksize 0 100000 \
  --family sqla_pandas sqla_arrow sqla_polars sqla_s3fs --repetitions 3 > results/jobs-xlarge-sqla-flat.json
jobs --suite single --scale xlarge --shape nested --chunksize 100000 \
  --family sqla_pandas sqla_arrow sqla_polars --repetitions 3 > results/jobs-xlarge-sqla-nested.json
uv run --no-sync python -m pyathena_bench --config results/fleet.toml queue \
  --stack "$BENCHMARK_STACK_ID" --name large-1 --jobs results/jobs-medium-*.json results/jobs-large-*.json \
  results/jobs-xlarge-*.json --manifest results/input-bounded.json
```

The manifest for this selection must be prepared with `--scale medium large xlarge`.
It is published as `large-1` in place of the previous example, so the worker and status commands below apply unchanged.
The selection has 96 jobs; from the job durations of the 2026-10-11 run, they take about 14 hours of job time on `r7g.2xlarge` hosts, and the longest, xlarge nested pandas CSV, about 75 minutes.

`queue` reserves the name with a conditional write, stores the configuration, manifest, and jobs under `fleet/<name>/` in the results location, and writes the `queue.json` marker last.
Workers refuse a queue without the marker, or wait for it with `--wait`.
A failed `queue` releases the reservation, so the same name can be published again; a `queue` process killed while publishing leaves `fleet/<name>/reservation.json`, which must be deleted before reusing the name.
Every worker runs its jobs with that stored configuration and manifest, so recovery and cleanup do not depend on the host where they were prepared.
The workers that the stack started begin as soon as `large-1` is published; follow them from the local machine:

```bash
uv run --env-file ../.env --locked python -m pyathena_bench status \
  --stack "$BENCHMARK_STACK" --name large-1
```

For a queue with another name, start a worker on every host with SSM; a second worker on a host whose worker still runs would overlap trials:

```bash
uv run --env-file ../.env --locked aws ssm send-command --targets "Key=tag:aws:autoscaling:groupName,Values=$BENCHMARK_GROUP" \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["sudo -iu ec2-user bash -lc \"cd /opt/pyathena/benchmarks && mkdir -p results && (nohup uv run --no-sync python -m pyathena_bench worker --results $(cat /opt/pyathena/benchmarks/results-location.txt) --name large-2 --terminate-host > results/worker-large-2.log 2>&1 &)\""]'
```

A worker claims a job by creating `claims/<job>` with an S3 conditional write, so exactly one host runs each job.
A claim, like each API slot under `slots/`, is a lease that its worker renews every tenth of `--lease-seconds` (600 by default, at least 300) while the job runs.
When a worker dies, its claim and slots expire, and another worker takes them over with a conditional write, so the job and the slots return to the fleet without manual steps.
A worker releases a slot by overwriting its lease as released, never by deleting it, so a slot that another worker took over stays held.
A worker whose claim was taken over, or whose renewals failed until a third of `--lease-seconds` was left, stops the attempt, which leaves its trial time to stop before another worker can take the claim over, quiesces its queries as after a failed job, and records nothing for the job; the new owner continues it, and the first `done` marker of a job is never replaced.
If that worker cannot confirm that the queries stopped, it raises an error and keeps its host, because the job may have no other worker.
A lost API slot does not stop the job; the `done` marker lists it under `lost_slots`.
Each attempt of a job writes to `results/<job>/attempt-<n>/`; between trials, its run writes each trial's start and record below `checkpoint/` there, so a lost host loses only its running trial, and the worker uploads the attempt's files when it ends.
A trial whose start checkpoint could not be written does not run, because the next attempt could not quiesce its queries; the run records it as failed, which fails the job.
After a job, the worker uploads the attempt's output directory and its log to `logs/`, renews its claim conditionally to confirm that it still owns the job, writes `job-summary.csv` and `job-summary.md` from every attempt into its attempt directory, and then creates `done/<job>` with the exit code, attempt, host, times, and the summary's attempt directory under `summary`.
A failed trial stops only its own job; `status` lists jobs with a non-zero exit code or a report error, the claims of running jobs, and the slots in use, each with its host, attempt, and whether its lease has expired.
After a failed job, the worker stops the queries that the job's trials reported, except those of trials recorded as successful.
Then it stops every queued or running query in the workgroup that writes under the output prefixes of those trials, which also covers queries whose IDs were never reported before a trial process stopped; this scans the workgroup's query history, and is skipped when every started trial succeeded.
It waits until they stop before claiming another job.
Each of these Athena requests is retried up to 10 times, compared with 5 elsewhere, and each retry waits with jittered exponential backoff of up to 20 seconds.
A history scan that fails does not skip the status checks of the queries already found.
If only throttling kept the queries from being confirmed as stopped, the worker quiesces them again, up to three times in total and a tenth of `--lease-seconds` apart, while it holds the claim.
If it cannot confirm that the queries stopped, it records the errors in the `done` marker and exits; after an interruption notice, it does not terminate the host.

Each worker checks instance metadata for a Spot interruption notice (`spot/instance-action`), which EC2 publishes about two minutes before it reclaims an instance.
On a notice, the worker interrupts the running trial with SIGINT, which cancels the trial's reported queries, uploads the attempt, releases its claim as `interrupted` and its slots, and exits without terminating the host.
A separate thread watches for the notice, so slow S3 requests do not delay it.
The group replaces the reclaimed instance, and the new host's worker takes over the interrupted claim as the next attempt: it downloads the earlier attempts, quiesces their trials as after a failed job, and runs its own warmups and then only the measured repetitions that they did not record as successful, through `run --resume`.
If Athena still throttles a request after its retries while the worker quiesces an earlier attempt, the worker releases the claim without running or recording the job and continues with the next pending job, or exits after an interruption notice, including one observed while it quiesced; a later attempt on any host quiesces the earlier attempts again, and the worker keeps its host while the job is pending.
The job's summary merges the attempts and leaves a failed or incomplete trial whose repetition succeeded in a later attempt out of `failed_trials`, noting its reason as `Superseded: ...`, so it reports the trials of an uninterrupted run without hiding failures.
`report results/recovered/fleet/<name>/results/<job>` produces the same merged summary from the recovered attempts.
Rebalance recommendations are not acted on; an interruption does not always follow them, and the notice leaves enough time to stop a trial.

Workers exit when no job is left to claim.
A claim whose worker died expires and is taken over by a worker that is still running; `status` marks it as expired.
With `--terminate-host`, a worker first suspends the group's `AZRebalance` process, which would otherwise let the group launch a host in one zone and terminate a busy host in another when Spot capacity or retired hosts leave the zones uneven; the process stays suspended until the stack is deleted.
A worker with `--terminate-host` that returns normally records its jobs, instance type, Availability Zone, and purchase option under `hosts/<instance-id>.json` and terminates its host through the group with a lower desired capacity, so no replacement starts and finished hosts stop costing money while others still run.
This includes a worker that stopped after unconfirmed queries, whose errors are already in the `done` marker.
A worker that raises an error, for example for a job wider than `--api-slots`, keeps its host.
To keep a host after its worker finishes, tag it with `KeepHost=true` (`aws ec2 create-tags --resources <instance-id> --tags Key=KeepHost,Value=true`); the worker reads the tag from instance metadata.
A job whose worker died after the other workers exited, or after a host error, stays claimed until its lease expires; then a worker started for the same queue, as below, takes it over.
To retry failed jobs, wait until all workers have exited, run `cleanup --trials-only --execute` for the manifest, and publish the selected jobs under a new queue name with the same manifest.

### Pilot with a simulated interruption

Before a fleet run, rehearse an interruption on one host.
Deploy with `FleetSize=1` and `QueueName=pilot-1`, prepare a small snapshot, and publish a job with several repetitions; the host's worker starts it:

```bash
BENCHMARK_STACK=pyathena-benchmark-pilot
uv run --env-file ../.env --locked aws cloudformation deploy \
  --stack-name "$BENCHMARK_STACK" \
  --template-file cloudformation/benchmark.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Purpose=pyathena-benchmark \
  --parameter-overrides GitCommit="$BENCHMARK_COMMIT" Architecture=arm64 InstanceType=r7g.2xlarge \
  ResultsBucket="$BENCHMARK_RESULTS_BUCKET" QueueName=pilot-1
# In a session on the host, as ec2-user in /opt/pyathena/benchmarks:
uv run --no-sync python -m pyathena_bench prepare \
  --stack "$(cat stack-id.txt)" --manifest results/input-pilot.json --scale small
uv run --no-sync python -m pyathena_bench jobs --suite single --scale small --family pandas --api sync \
  --transport csv --output-kind native --warmups 0 --repetitions 6 > results/jobs-pilot.json
uv run --no-sync python -m pyathena_bench queue \
  --stack "$(cat stack-id.txt)" --name pilot-1 --jobs results/jobs-pilot.json --manifest results/input-pilot.json
```

When `status` shows the job claimed and `results/<job>/attempt-1/checkpoint/` has a trial record, create the file `interruption-notice` in the worker's directory, `/opt/pyathena/benchmarks/results/fleet/pilot-1/`, which the worker treats as an interruption notice and removes when it starts.
As after a real notice, the worker exits and leaves its host running, so terminate the host as below.
Check that the claim is `interrupted`, then terminate the host without lowering the desired capacity, as EC2 does when it reclaims a Spot Instance:

```bash
BENCHMARK_GROUP=$(uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`AutoScalingGroup`].OutputValue | [0]' --output text)
BENCHMARK_INSTANCE=$(uv run --env-file ../.env --locked aws autoscaling describe-auto-scaling-groups \
  --auto-scaling-group-names "$BENCHMARK_GROUP" --query 'AutoScalingGroups[0].Instances[0].InstanceId' --output text)
uv run --env-file ../.env --locked aws ssm send-command --instance-ids "$BENCHMARK_INSTANCE" \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["sudo -u ec2-user touch /opt/pyathena/benchmarks/results/fleet/pilot-1/interruption-notice"]'
uv run --env-file ../.env --locked python -m pyathena_bench status --stack "$BENCHMARK_STACK" --name pilot-1
uv run --env-file ../.env --locked aws autoscaling terminate-instance-in-auto-scaling-group \
  --instance-id "$BENCHMARK_INSTANCE" --no-should-decrement-desired-capacity
```

The group launches a replacement whose worker continues the job as `attempt-2` and then retires the host.
The pilot passes when `done/<job>` records `attempt` 2 and exit code 0, every case in `results/<job>/attempt-2/job-summary.csv` reports six successful trials and no failures, `hosts/` records the replacement with lifecycle `spot`, and the group's desired capacity drops to zero.
AWS Fault Injection Service (`aws:ec2:send-spot-instance-interruptions`) sends a real notice instead, but needs its own IAM role and experiment template.

## Reports and recovery

Every output directory contains `environment.json`, a copy of the input manifest, `events.jsonl`, and `trials.jsonl`.
Environment metadata includes Git SHA, dirty state, Python and dependency versions, lockfile hash, CPU count, RAM, and settings.
SDK events preserve query IDs, SQL, output locations, Athena statistics, and failed operations without storing result rows.
Successful measurements generate `summary.csv` and `summary.md`; failed trials and unsupported cases remain visible.
After each successful measured UNLOAD or CTAS trial, `run` reads the query manifests (`<query_id>-manifest.csv`) below the trial's output location and records the number and sizes of the data files they list in `output_files`; the summary shows the most files and the largest file of the measured trials, and notes a measured trial whose files could not be summarized.
Regenerate reports after interruption with:

```bash
uv run --no-sync python -m pyathena_bench report results/single-small
```

Report recovery ignores an incomplete final JSONL record with a warning and marks started trials without a final record as incomplete.
For a fleet job directory, `report results/recovered/fleet/<name>/results/<job>` merges the trials of its `attempt-<n>` subdirectories.
Malformed records elsewhere remain errors.

Small sample sizes make tail percentiles descriptive rather than reliable population estimates.
Retain the raw files alongside the summary.
Results are gitignored and are not uploaded by CI.

## Recovering reports and deleting the environment

Stop the benchmark process before cleanup or stack deletion.
Recover the fleet queues and reports first, including failed-run evidence; with an expiring results bucket, do so within a day of the run.
Each queue holds its manifest, configuration, and jobs, so no host needs to be kept for recovery.
Local files on a remaining host, such as a manifest that was never queued, can go through the stack's generated bucket; it is separate from `pyathena-benchmark`, which holds the input data.

On EC2, only for such local files:

```bash
BENCHMARK_BUCKET=$(aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK_ID" \
  --query 'Stacks[0].Outputs[?OutputKey==`Bucket`].OutputValue | [0]' --output text)
aws s3 sync results/ "s3://$BENCHMARK_BUCKET/reports/"
```

On the local machine, from `benchmarks/`:

```bash
BENCHMARK_BUCKET=$(uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`Bucket`].OutputValue | [0]' --output text)
mkdir -p results/recovered
uv run --env-file ../.env --locked aws s3 sync "s3://$BENCHMARK_BUCKET/reports/" results/recovered/
BENCHMARK_RESULTS=$(uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`Results`].OutputValue | [0]' --output text)
uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" > results/recovered/stack.json
uv run --env-file ../.env --locked aws s3 sync "${BENCHMARK_RESULTS}fleet/" results/recovered/fleet/
uv run --env-file ../.env --locked aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=$BENCHMARK_GROUP" > results/recovered/instances.json
```

Verify the downloaded files before proceeding.
For every preparation manifest, preview and then clean that run using its original configuration; a queued manifest is `results/recovered/fleet/<name>/manifest.json`:

```bash
uv run --env-file ../.env --locked python -m pyathena_bench cleanup \
  --manifest results/recovered/fleet/large-1/manifest.json
uv run --env-file ../.env --locked python -m pyathena_bench cleanup \
  --manifest results/recovered/fleet/large-1/manifest.json --execute
```

Cleanup checks the live stack identity, cancels and waits for matching active queries, removes run-prefixed scratch tables, aborts matching multipart uploads, and removes the run's prefix `runs/<run_id>/` in the scratch bucket and in the results location.
History discovery uses [batches of up to 50 query IDs](https://docs.aws.amazon.com/athena/latest/APIReference/API_BatchGetQueryExecution.html); completed queries need no cancellation calls.
Unavailable recorded query metadata produces a warning and falls back to scanning live history; errors inspecting that history stop cleanup before deletion.
The transfer copy under `reports/` remains until the final stack teardown.
For multiple manifests, repeat cleanup for each; never clean while another process is still submitting queries.

Confirm the scratch database is empty and the only remaining S3 objects are the recovered reports and fleet queues.
In a separate results bucket, the fleet queues under `fleet/` remain until they expire, or until they are removed with `aws s3 rm "${BENCHMARK_RESULTS}fleet/" --recursive`.
An unexpected table or object is a reason to inspect the corresponding run before deleting anything further.

```bash
BENCHMARK_DATABASE=$(uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" \
  --query 'Stacks[0].Outputs[?OutputKey==`ScratchDatabase`].OutputValue | [0]' --output text)
uv run --env-file ../.env --locked aws glue get-tables --database-name "$BENCHMARK_DATABASE" --query 'TableList[].Name'
uv run --env-file ../.env --locked aws s3 ls "s3://$BENCHMARK_BUCKET/" --recursive
uv run --env-file ../.env --locked aws s3 rm "s3://$BENCHMARK_BUCKET/reports/" --recursive
uv run --env-file ../.env --locked aws s3 rm "s3://$BENCHMARK_BUCKET/fleet/" --recursive
uv run --env-file ../.env --locked aws s3api list-multipart-uploads --bucket "$BENCHMARK_BUCKET"
uv run --env-file ../.env --locked aws s3 ls "s3://$BENCHMARK_BUCKET/" --recursive
uv run --env-file ../.env --locked aws cloudformation delete-stack --stack-name "$BENCHMARK_STACK"
uv run --env-file ../.env --locked aws cloudformation wait stack-delete-complete --stack-name "$BENCHMARK_STACK"
```

CloudFormation can delete an S3 bucket only when it is empty.
The template does not retain EBS, S3, or other benchmark resources, and it does not include an automatic bucket-emptying Lambda.
The source table, source data, and existing workgroup are not owned by this stack and remain intact.
The local recovered reports are the retained benchmark evidence.
If bootstrap fails, inspect the stack events; no benchmark data is created during bootstrap.
Default rollback removes the instances and their local logs.
For bootstrap diagnosis, deploy with `--disable-rollback`, inspect `/var/log/cloud-init-output.log` while the instances exist, and explicitly delete the failed stack afterward.
