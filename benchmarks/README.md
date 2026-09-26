<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Cursor benchmarks

This project measures result retrieval, memory use, and concurrent query behavior for PyAthena and AWS SDK for pandas (AWS Wrangler).
It includes a disposable CloudFormation environment and does not run benchmarks in CI.
Measurements and cursor recommendations for this harness are pending.

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
Neither workload converts every library's output into a common Python object representation.

## Dependencies and local checks

The benchmark is a non-packaged member of the repository's uv workspace and runs on Python 3.12.
It shares the root `uv.lock` with PyAthena, which it uses from the workspace checkout.
Locally and in CI, its virtual environment is `benchmarks/.venv`, separate from the root `.venv`.
On the EC2 instance, the dedicated checkout's root `/opt/pyathena/.venv` holds the benchmark environment.
The root `uv build -v` still builds only PyAthena, including a wheel built from its sdist.
Benchmark dependencies do not become PyAthena runtime dependencies, and `uv sync --group dev` at the root does not install them.

From the repository root:

```bash
just benchmark lint
just benchmark test
```

The recipes select `benchmarks/.venv` and Python 3.12, and sync that environment from the locked workspace.
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
Instance size and disk size are parameters, so a later run can deliberately test another memory budget.
The AMI parameter resolves the current AL2023 image at deployment; record the resulting AMI when comparing environments.
Source bucket access is read-only and restricted to the configured prefix.
The instances can write only to the scratch bucket and database, and can query the specified existing workgroup.
The supplied template assumes ordinary IAM access and SSE-S3 source objects; Lake Formation restrictions or a customer-managed KMS key require corresponding grants before execution.

The deployment identity needs permission to create these resources and pass the instance role.
Use an immutable, remotely accessible commit that contains this directory and the root `uv.lock`.
Bootstrap checks out that commit, installs pinned uv and Python versions, and runs `uv sync --locked --no-dev` for the benchmark into `/opt/pyathena/.venv`.
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
| `total_seconds` | Execute plus full result consumption; excludes connection preparation and cleanup |
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
Polars lazy CSV scans of S3 objects download the whole object into a file cache in this directory before yielding batches; in the recorded runs, those files remained after the process exited.
When the temporary directory is a tmpfs, as `/tmp` is on Amazon Linux 2023, this storage uses memory that RSS does not include.

| Capability | Treatment |
| --- | --- |
| Cursor, DictCursor, S3FS | Row batches with arraysize 100 and 1000 |
| Pandas CSV | Full DataFrame or streamed chunks |
| Pandas UNLOAD | Full Parquet read; requested chunked cases are marked unsupported as bounded-memory measurements |
| Arrow CSV and UNLOAD | Eager table loading; arraysize changes row extraction, not S3 streaming |
| Polars CSV and UNLOAD | Full result or chunk iterator |
| Wrangler CSV | Scalar projections; nested cases are marked unsupported |
| Wrangler CTAS and UNLOAD | Full DataFrame or chunks, including nested workloads |

The [Wrangler API documentation](https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.athena.read_sql_query.html) describes its CSV nested-type restriction.
CTAS and UNLOAD have separate limitations, including timestamp-with-time-zone columns; the supplied timestamp is a bigint.
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

Deploy a new stack with the fleet size under its own name; changing `FleetSize` on an existing stack does not wait for the added hosts to finish bootstrap.
Then open a session on one of its hosts:

```bash
BENCHMARK_STACK=pyathena-benchmark-fleet
uv run --env-file ../.env --locked aws cloudformation deploy \
  --stack-name "$BENCHMARK_STACK" \
  --template-file cloudformation/benchmark.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Purpose=pyathena-benchmark \
  --parameter-overrides GitCommit="$BENCHMARK_COMMIT" FleetSize=20
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

`queue` reserves the name with a conditional write, stores the configuration, manifest, and jobs under `fleet/<name>/` in the scratch bucket, and writes the `queue.json` marker last.
Workers refuse a queue without the marker.
A failed `queue` releases the reservation, so the same name can be published again; a `queue` process killed while publishing leaves `fleet/<name>/reservation.json`, which must be deleted before reusing the name.
Every worker runs its jobs with that stored configuration.
Start a worker on every host from the local machine:

```bash
uv run --env-file ../.env --locked aws ssm send-command --targets "Key=tag:aws:autoscaling:groupName,Values=$BENCHMARK_GROUP" \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["sudo -iu ec2-user bash -lc \"cd /opt/pyathena/benchmarks && mkdir -p results && (nohup uv run --no-sync python -m pyathena_bench worker --stack $(cat /opt/pyathena/benchmarks/stack-id.txt) --name large-1 > results/worker-large-1.log 2>&1 &)\""]'
uv run --env-file ../.env --locked python -m pyathena_bench status \
  --stack "$BENCHMARK_STACK" --name large-1
```

A worker claims a job by creating `claims/<job>` with an S3 conditional write, so exactly one host runs each job.
After a job, the worker uploads its output directory to `results/<job>/` and its log to `logs/`, then writes `done/<job>` with the exit code, host, and times.
A failed trial stops only its own job; `status` lists jobs with a non-zero exit code.
After a failed job, the worker stops the job's reported queries and every queued or running query in the workgroup that writes under the job's trial output prefixes, which also covers queries whose IDs were never reported before a trial process died.
It waits until they stop before claiming another job; this scans the workgroup's query history.
If it cannot confirm that, it records the errors in the `done` marker and exits.
A worker that dies leaves a claim without a `done` marker, and it can also leave an API slot under `slots/`, which lowers the fleet's API-heavy capacity.
`status` lists slots in use; delete a slot object only after confirming that the host named in it no longer runs a job.
Workers exit when every job has been claimed.
To retry jobs, wait until all workers have exited, run `cleanup --trials-only --execute` for the manifest, and publish the selected jobs under a new queue name with the same manifest.
Download `fleet/<name>/` with `aws s3 sync` before cleanup and stack deletion, as described below.

## Reports and recovery

Every output directory contains `environment.json`, a copy of the input manifest, `events.jsonl`, and `trials.jsonl`.
Environment metadata includes Git SHA, dirty state, Python and dependency versions, lockfile hash, CPU count, RAM, and settings.
SDK events preserve query IDs, SQL, output locations, Athena statistics, and failed operations without storing result rows.
Successful measurements generate `summary.csv` and `summary.md`; failed trials and unsupported cases remain visible.
Regenerate reports after interruption with:

```bash
uv run --no-sync python -m pyathena_bench report results/single-small
```

Report recovery ignores an incomplete final JSONL record with a warning and marks started trials without a final record as incomplete.
Malformed records elsewhere remain errors.

Small sample sizes make tail percentiles descriptive rather than reliable population estimates.
Retain the raw files alongside the summary.
Results are gitignored and are not uploaded by CI.

## Recovering reports and deleting the environment

Stop the benchmark process before cleanup or stack deletion.
Recover all local manifests and reports first, including failed-run evidence.
Use the stack's generated bucket as a temporary transfer location; it is separate from `pyathena-benchmark`, which holds the input data.

On EC2:

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
uv run --env-file ../.env --locked aws cloudformation describe-stacks --stack-name "$BENCHMARK_STACK" > results/recovered/stack.json
uv run --env-file ../.env --locked aws s3 sync "s3://$BENCHMARK_BUCKET/fleet/" results/recovered/fleet/
uv run --env-file ../.env --locked aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=$BENCHMARK_GROUP" > results/recovered/instances.json
```

Verify the downloaded files before proceeding.
For every preparation manifest, preview and then clean that run using its original configuration:

```bash
uv run --env-file ../.env --locked python -m pyathena_bench cleanup \
  --manifest results/recovered/input.json
uv run --env-file ../.env --locked python -m pyathena_bench cleanup \
  --manifest results/recovered/input.json --execute
```

Cleanup checks the live stack identity, cancels and waits for matching active queries, removes run-prefixed scratch tables, aborts matching multipart uploads, and removes the run's S3 prefix.
History discovery uses [batches of up to 50 query IDs](https://docs.aws.amazon.com/athena/latest/APIReference/API_BatchGetQueryExecution.html); completed queries need no cancellation calls.
Unavailable recorded query metadata produces a warning and falls back to scanning live history; errors inspecting that history stop cleanup before deletion.
The transfer copy under `reports/` remains until the final stack teardown.
For multiple manifests, repeat cleanup for each; never clean while another process is still submitting queries.

Confirm the scratch database is empty and the only remaining S3 objects are the recovered reports and fleet queues.
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
