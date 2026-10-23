# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Command line entry point for explicit preparation and measurement."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, replace
from pathlib import Path

from pyathena_bench.aws import (
    cleanup,
    client,
    preflight,
    prepare,
    prepare_fixtures,
    prepare_object_sets,
    prepare_objects,
    run_locations,
    session,
    split_location,
    stack_resources,
    validate_manifest,
)
from pyathena_bench.cases import (
    ALL_FAMILIES,
    FILESYSTEM_OPERATIONS,
    OBJECTS_OPERATIONS,
    SHAPES,
    SUITES,
    matrix,
)
from pyathena_bench.config import Settings, read_json
from pyathena_bench.fleet import (
    API_FAMILIES,
    ENVIRONMENTS,
    Filters,
    Queue,
    SpotInterruptionError,
    expand_jobs,
    merge_jobs,
    retire_host,
    status,
    submit,
    suspend_rebalance,
    work,
)
from pyathena_bench.report import report
from pyathena_bench.runner import completed_trials, run, s3_checkpoint


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description="Athena cursor benchmarks (AWS operations are explicit)"
    )
    root.add_argument("--config", type=Path, default=Path("config.toml"))
    root.add_argument("--profile", help="Local AWS profile; omit on EC2")
    commands = root.add_subparsers(dest="command", required=True)
    check = commands.add_parser(
        "preflight", help="Read stack and input metadata without querying Athena"
    )
    check.add_argument("--stack", required=True)
    prep = commands.add_parser("prepare", help="Create fixed inputs; incurs Athena and S3 usage")
    prep.add_argument("--stack", required=True)
    prep.add_argument("--manifest", type=Path, required=True)
    prep.add_argument(
        "--scale",
        nargs="*",
        default=[],
        help="Snapshots to create; none for a manifest that only the filesystem suite uses",
    )
    prep.add_argument(
        "--filesystem-scale",
        nargs="+",
        default=[],
        help="Source objects of the filesystem suite to upload, which fleet hosts then share",
    )
    prep.add_argument(
        "--objects-scale",
        nargs="+",
        default=[],
        help="Object sets of the objects suite to upload, which fleet hosts then share",
    )
    for name in ("plan", "run", "jobs"):
        command = commands.add_parser(name)
        if name == "jobs":
            command.add_argument("--suite", choices=SUITES, nargs="+", required=True)
            command.add_argument("--shape", choices=SHAPES, nargs="+", default=["flat"])
            command.add_argument(
                "--split-pages",
                type=int,
                help="Run each repetition as a separate job when one trial needs this many "
                "GetQueryResults pages",
            )
            command.add_argument(
                "--environment",
                choices=ENVIRONMENTS,
                help="Virtual environment beside the default one that runs the jobs",
            )
        else:
            command.add_argument("--suite", choices=SUITES, required=True)
            command.add_argument("--shape", choices=SHAPES, default="flat")
        command.add_argument("--warmups", type=int, help="Override the configured warmups")
        command.add_argument("--repetitions", type=int, help="Override the configured repetitions")
        command.add_argument("--scale", nargs="+", required=True)
        command.add_argument("--family", choices=ALL_FAMILIES, nargs="+")
        command.add_argument(
            "--api", choices=("sync", "thread", "aio", "direct", "to_thread", "s3fs"), nargs="+"
        )
        command.add_argument(
            "--transport",
            # The two suites share the get, put, and copy operation names.
            choices=("csv", "unload", "ctas", *{*FILESYSTEM_OPERATIONS, *OBJECTS_OPERATIONS}),
            nargs="+",
            help="Result transports, or the operations of the filesystem and objects suites",
        )
        command.add_argument("--output-kind", choices=("rows", "native", "bytes", "objects"))
        command.add_argument("--arraysize", type=int, nargs="+")
        command.add_argument(
            "--chunksize",
            type=int,
            nargs="+",
            help="Chunk sizes to keep; 0 selects unchunked reads",
        )
        if name == "run":
            command.add_argument("--manifest", type=Path, required=True)
            command.add_argument("--out", type=Path, required=True)
            command.add_argument(
                "--resume",
                type=Path,
                nargs="+",
                default=[],
                help="Output directories of earlier attempts whose finished trials are skipped",
            )
            command.add_argument(
                "--checkpoint",
                help="s3:// prefix below which each trial's start and record are written "
                "between trials; set by fleet workers, whose job summary merges the attempts",
            )
    enqueue = commands.add_parser(
        "queue", help="Publish jobs and a prepared manifest for fleet workers"
    )
    enqueue.add_argument("--stack", required=True)
    enqueue.add_argument("--name", required=True)
    enqueue.add_argument(
        "--jobs", type=Path, nargs="+", required=True, help="Job files to merge into one queue"
    )
    enqueue.add_argument("--manifest", type=Path, required=True)
    consume = commands.add_parser("worker", help="Run queued jobs on this host until none remain")
    location = consume.add_mutually_exclusive_group(required=True)
    location.add_argument("--stack", help="Stack whose Results output holds the queue")
    location.add_argument(
        "--results", help="The stack's results location, which hosts know from their user data"
    )
    consume.add_argument("--name", required=True)
    consume.add_argument(
        "--api-slots",
        type=int,
        default=10,
        help="Fleet-wide limit for jobs that page through GetQueryResults",
    )
    consume.add_argument("--workdir", type=Path)
    consume.add_argument(
        "--terminate-host",
        action="store_true",
        help="Suspend zone rebalancing at start, and terminate this host through its Auto "
        "Scaling group when no job is left to claim",
    )
    consume.add_argument(
        "--wait",
        action="store_true",
        help="Wait for the queue to be published instead of failing",
    )
    consume.add_argument(
        "--lease-seconds",
        type=float,
        default=600.0,
        help="Seconds after which a dead worker's claim and API slots return to the fleet",
    )
    progress = commands.add_parser("status", help="Summarize a fleet queue")
    progress.add_argument("--stack", required=True)
    progress.add_argument("--name", required=True)
    summary = commands.add_parser(
        "report", help="Generate CSV and Markdown from local measurements"
    )
    summary.add_argument("directory", type=Path)
    clean = commands.add_parser(
        "cleanup", help="Preview or delete one manifest's temporary resources"
    )
    clean.add_argument("--manifest", type=Path, required=True)
    clean.add_argument("--execute", action="store_true")
    clean.add_argument(
        "--trials-only",
        action="store_true",
        help="Keep fixed inputs and initialization fixtures for retry",
    )
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "report":
        report(args.directory)
        return 0
    settings = Settings.load(args.config)
    if args.profile:
        settings = replace(settings, profile=args.profile)
    scales = getattr(args, "scale", [])
    suite = getattr(args, "suite", None)
    # prepare has no suite: --scale names snapshots, --filesystem-scale objects, and
    # --objects-scale object sets.
    selections = [
        (scales, settings.suite_scales(name))
        for name in (suite if isinstance(suite, list) else [suite])
    ]
    selections.append((getattr(args, "filesystem_scale", []), settings.filesystem_sizes))
    selections.append((getattr(args, "objects_scale", []), settings.objects_counts))
    if any(
        len(set(names)) != len(names) or any(n not in defined for n in names)
        for names, defined in selections
    ):
        raise ValueError("Select distinct scales defined in the configuration")
    if args.command == "preflight":
        sys.stdout.write(json.dumps(preflight(settings, args.stack), indent=2) + "\n")
    elif args.command == "prepare":
        manifest = prepare(settings, args.stack, args.manifest, scales)
        if args.filesystem_scale:
            prepare_objects(settings, manifest, args.manifest, args.filesystem_scale)
        if args.objects_scale:
            prepare_object_sets(settings, manifest, args.manifest, args.objects_scale)
    elif args.command == "cleanup":
        manifest = read_json(args.manifest)
        resources = validate_manifest(settings, manifest)
        sys.stdout.write(
            json.dumps(
                {
                    "stack": resources["StackId"],
                    "database": resources["ScratchDatabase"],
                    "table_selector": (
                        f"b_{manifest['run_id']}_<32-hex-trial-id> (excluding input tables)"
                        if args.trials_only
                        else f"b_{manifest['run_id']}_*"
                    ),
                    "s3_prefixes": [
                        root + ("trials/" if args.trials_only else "")
                        for root in sorted(
                            set(run_locations(resources, manifest["run_id"]).values())
                        )
                    ],
                    "preserve_inputs_and_fixtures": args.trials_only,
                },
                indent=2,
            )
            + "\n"
        )
        if args.execute:
            cleanup(settings, manifest, args.manifest, trials_only=args.trials_only)
    elif args.command in {"queue", "worker", "status"}:
        session_ = session(settings)
        results = getattr(args, "results", None) or stack_resources(session_, args.stack)["Results"]
        bucket, base = split_location(results)
        queue = Queue(client(session_, "s3"), bucket, args.name, base)
        if args.command == "queue":
            validate_manifest(settings, read_json(args.manifest))
            submit(queue, merge_jobs(read_json(p) for p in args.jobs), args.config, args.manifest)
        elif args.command == "worker":
            if args.api_slots < 1:
                raise ValueError("--api-slots must be positive")
            if args.lease_seconds < 300:
                raise ValueError("--lease-seconds must be at least 300")
            autoscaling = client(session_, "autoscaling")
            if args.terminate_host:
                suspend_rebalance(autoscaling)
            try:
                ran = work(
                    queue,
                    args.workdir or Path("results/fleet") / args.name,
                    args.api_slots,
                    lease_seconds=args.lease_seconds,
                    wait=args.wait,
                )
            except SpotInterruptionError as interrupted:
                # The host is being reclaimed, and its group launches a replacement; after
                # a rehearsed notice, the operator terminates the host the same way.
                sys.stdout.write(
                    json.dumps({"ran": interrupted.ran, "interrupted": True}, indent=2) + "\n"
                )
                return 0
            sys.stdout.write(json.dumps({"ran": ran}, indent=2) + "\n")
            if args.terminate_host:
                # Only a normal return retires the host; an error keeps it for inspection.
                retire_host(queue, autoscaling, ran)
        else:
            sys.stdout.write(json.dumps(status(queue), indent=2) + "\n")
    else:
        filters = Filters(
            args.family, args.api, args.transport, args.output_kind, args.arraysize, args.chunksize
        )
        overrides = {
            k: v for k in ("warmups", "repetitions") if (v := getattr(args, k)) is not None
        }
        if overrides:
            settings = replace(settings, **overrides)
    if args.command == "jobs":
        jobs = expand_jobs(
            settings,
            args.suite,
            scales,
            args.shape,
            filters,
            args.split_pages,
            explicit_trials=bool(overrides),
            environment=args.environment,
        )
        sys.stdout.write(json.dumps(jobs, indent=1) + "\n")
    elif args.command in {"plan", "run"}:
        cases = filters.select(matrix(settings, args.suite, args.shape))
        if not cases:
            raise ValueError("No cases match the selected filters")
        if args.command == "plan":
            page_size = min(
                (c.arraysize for c in cases if c.family in API_FAMILIES and c.suite != "init"),
                default=0,
            )
            sys.stdout.write(
                json.dumps(
                    {
                        "scales": {s: settings.suite_scales(args.suite)[s] for s in scales},
                        "trials": sum(
                            c.unsupported_reason(settings, s) is None for c in cases for s in scales
                        )
                        * (settings.warmups + settings.repetitions),
                        "cases": [{"id": c.id, **asdict(c)} for c in cases],
                        "warnings": [
                            f"{scale}: arraysize {page_size} needs >= "
                            f"{(settings.scales[scale] + page_size - 1) // page_size} "
                            "GetQueryResults pages per query; use a pilot to choose timeout_seconds"
                            for scale in scales
                            if page_size and settings.scales[scale] >= 1_000_000
                        ],
                    },
                    indent=2,
                )
                + "\n"
            )
        else:
            if args.out.exists():
                raise ValueError("Output directory already exists")
            manifest = read_json(args.manifest)
            if args.suite == "init":
                prepare_fixtures(settings, manifest, args.manifest, scales, args.shape)
            elif args.suite == "filesystem":
                prepare_objects(settings, manifest, args.manifest, scales)
            elif args.suite == "objects":
                prepare_object_sets(settings, manifest, args.manifest, scales)
            checkpoint = (
                s3_checkpoint(client(session(settings), "s3"), args.checkpoint)
                if args.checkpoint
                else None
            )
            success = run(
                settings,
                manifest,
                cases,
                scales,
                args.out,
                completed_trials(args.resume),
                checkpoint,
            )
            if not checkpoint:
                # A fleet job's summary merges its attempts instead.
                report(args.out)
            return 0 if success else 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError) as exc:
        logging.error("%s", exc)
        sys.exit(1)
