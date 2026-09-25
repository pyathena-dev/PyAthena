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
    session,
    stack_resources,
    validate_manifest,
)
from pyathena_bench.cases import FAMILIES, matrix
from pyathena_bench.config import Settings, read_json
from pyathena_bench.fleet import Filters, Queue, expand_jobs, status, submit, work
from pyathena_bench.report import report
from pyathena_bench.runner import run


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
    prep.add_argument("--scale", nargs="+", required=True)
    for name in ("plan", "run", "jobs"):
        command = commands.add_parser(name)
        suites = ("single", "concurrent", "init")
        if name == "jobs":
            command.add_argument("--suite", choices=suites, nargs="+", required=True)
            command.add_argument("--shape", choices=("flat", "nested"), nargs="+", default=["flat"])
            command.add_argument(
                "--split-pages",
                type=int,
                help="Run each repetition as a separate job when one trial needs this many "
                "GetQueryResults pages",
            )
        else:
            command.add_argument("--suite", choices=suites, required=True)
            command.add_argument("--shape", choices=("flat", "nested"), default="flat")
            command.add_argument("--warmups", type=int, help="Override the configured warmups")
            command.add_argument(
                "--repetitions", type=int, help="Override the configured repetitions"
            )
        command.add_argument("--scale", nargs="+", required=True)
        command.add_argument("--family", choices=(*FAMILIES, "wrangler"), nargs="+")
        command.add_argument(
            "--api", choices=("sync", "thread", "aio", "direct", "to_thread"), nargs="+"
        )
        command.add_argument("--transport", choices=("csv", "unload", "ctas"), nargs="+")
        command.add_argument("--output-kind", choices=("rows", "native"))
        command.add_argument("--arraysize", type=int, nargs="+")
        if name == "run":
            command.add_argument("--manifest", type=Path, required=True)
            command.add_argument("--out", type=Path, required=True)
    enqueue = commands.add_parser(
        "queue", help="Publish jobs and a prepared manifest for fleet workers"
    )
    enqueue.add_argument("--stack", required=True)
    enqueue.add_argument("--name", required=True)
    enqueue.add_argument("--jobs", type=Path, required=True)
    enqueue.add_argument("--manifest", type=Path, required=True)
    consume = commands.add_parser("worker", help="Run queued jobs on this host until none remain")
    consume.add_argument("--stack", required=True)
    consume.add_argument("--name", required=True)
    consume.add_argument(
        "--api-slots",
        type=int,
        default=10,
        help="Fleet-wide limit for jobs that page through GetQueryResults",
    )
    consume.add_argument("--workdir", type=Path)
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
    if len(set(scales)) != len(scales) or any(s not in settings.scales for s in scales):
        raise ValueError("Select distinct scales defined in the configuration")
    if args.command == "preflight":
        sys.stdout.write(json.dumps(preflight(settings, args.stack), indent=2) + "\n")
    elif args.command == "prepare":
        prepare(settings, args.stack, args.manifest, scales)
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
                    "s3_prefix": f"s3://{resources['Bucket']}/runs/{manifest['run_id']}/"
                    + ("trials/" if args.trials_only else ""),
                    "preserve_inputs_and_fixtures": args.trials_only,
                    "query_output_prefix": f"s3://{resources['Bucket']}/runs/{manifest['run_id']}/",
                },
                indent=2,
            )
            + "\n"
        )
        if args.execute:
            cleanup(settings, manifest, args.manifest, trials_only=args.trials_only)
    elif args.command in {"queue", "worker", "status"}:
        session_ = session(settings)
        queue = Queue(
            client(session_, "s3"), stack_resources(session_, args.stack)["Bucket"], args.name
        )
        if args.command == "queue":
            validate_manifest(settings, read_json(args.manifest))
            submit(queue, read_json(args.jobs), args.config, args.manifest)
        elif args.command == "worker":
            if args.api_slots < 1:
                raise ValueError("--api-slots must be positive")
            ran = work(queue, args.workdir or Path("results/fleet") / args.name, args.api_slots)
            sys.stdout.write(json.dumps({"ran": ran}, indent=2) + "\n")
        else:
            sys.stdout.write(json.dumps(status(queue), indent=2) + "\n")
    elif args.command == "jobs":
        filters = Filters(args.family, args.api, args.transport, args.output_kind, args.arraysize)
        jobs = expand_jobs(settings, args.suite, scales, args.shape, filters, args.split_pages)
        sys.stdout.write(json.dumps(jobs, indent=1) + "\n")
    else:
        overrides = {
            k: v for k in ("warmups", "repetitions") if (v := getattr(args, k)) is not None
        }
        if overrides:
            settings = replace(settings, **overrides)
        filters = Filters(args.family, args.api, args.transport, args.output_kind, args.arraysize)
        cases = filters.select(matrix(settings, args.suite, args.shape))
        if not cases:
            raise ValueError("No cases match the selected filters")
        if args.command == "plan":
            page_size = min(
                (
                    c.arraysize
                    for c in cases
                    if c.family in {"cursor", "dict"} and c.suite != "init"
                ),
                default=0,
            )
            sys.stdout.write(
                json.dumps(
                    {
                        "scales": {s: settings.scales[s] for s in scales},
                        "trials": sum(c.unsupported is None for c in cases)
                        * len(scales)
                        * (settings.warmups + settings.repetitions),
                        "cases": [{"id": c.id, **asdict(c)} for c in cases],
                        "warnings": [
                            f"{scale}: arraysize {page_size} needs >= "
                            f"{(settings.scales[scale] + page_size - 1) // page_size} "
                            "GetQueryResults pages per query; use a pilot to choose timeout_seconds"
                            for scale in scales
                            if settings.scales[scale] >= 1_000_000 and page_size
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
            success = run(settings, manifest, cases, scales, args.out)
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
