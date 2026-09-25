# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Remove expired test databases and S3 Tables namespaces left by test sessions."""

# Usage (from the repository root, with Boto3 credentials and region configured):
#   uv run --locked --no-dev python scripts/sweep_databases.py
# Add --apply to delete eligible metadata; the default only previews counts.
# AWS_PROFILE and AWS_DEFAULT_REGION can select the account and region.
#
# Eligible databases must exactly match a PyAthena or SQLAlchemy fixture name
# and have a creation time more than seven days old. Resource links and federated
# databases are excluded. The script completes inventory before deleting and
# rechecks eligibility and creation time immediately before each deletion.
# Only missing-database errors are ignored; other API failures stop the sweep.
# Deletion removes Glue database and table metadata, not S3 objects.
#
# With AWS_ATHENA_S3_TABLES_CATALOG set (s3tablescatalog/<table-bucket>), the
# script also sweeps that table bucket's namespaces named like PyAthena test
# schemas and more than seven days old, deleting their tables first. Test
# sessions create and delete such a namespace; a session that stops early
# leaves it behind.
#
# .github/workflows/database-sweep.yaml runs this script after scheduled Test
# runs complete on master, including failures and cancellations. It does not run
# after PR tests or manually dispatched tests. Manual sweep dispatch on master
# defaults to preview. The job has a 15-minute timeout; a timeout or API failure
# can leave eligible databases for a later run.

import argparse
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.config import Config

_LOGGER = logging.getLogger(__name__)
_TEST_DATABASE = re.compile(
    r"(?:pyathena_test_[a-z0-9]{10}|test_[0-9a-f]{12}(?:_test_schema(?:_2)?)?)"
)
_TEST_NAMESPACE = re.compile(r"pyathena_test_[a-z0-9]{10}")


def _eligible(database: dict[str, Any], cutoff: datetime) -> bool:
    created = database.get("CreateTime")
    return (
        _TEST_DATABASE.fullmatch(database["Name"]) is not None
        and isinstance(created, datetime)
        and created.tzinfo is not None
        and created < cutoff
        and not database.get("TargetDatabase")
        and not database.get("FederatedDatabase")
    )


def sweep_databases(client: Any, catalog_id: str, *, dry_run: bool = True) -> dict[str, int]:
    """Preview or delete test databases older than seven days.

    Fixtures generate fresh database names for each session or worker.
    Databases younger than seven days are retained, including concurrent CI runs.
    Only Glue metadata is deleted; S3 objects and child catalogs are untouched.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    # Finish pagination before deleting anything from the catalog.
    candidates = [
        database
        for page in client.get_paginator("get_databases").paginate(CatalogId=catalog_id)
        for database in page["DatabaseList"]
        if _eligible(database, cutoff)
    ]
    deleted = skipped = 0
    for database in candidates:
        if dry_run:
            continue
        try:
            current = client.get_database(CatalogId=catalog_id, Name=database["Name"])["Database"]
            # Preserve databases recreated or turned into references since listing.
            if not _eligible(current, cutoff) or current["CreateTime"] != database["CreateTime"]:
                skipped += 1
                continue
            client.delete_database(CatalogId=catalog_id, Name=database["Name"])
            deleted += 1
            time.sleep(0.25)
        except client.exceptions.EntityNotFoundException:
            skipped += 1
    return {"eligible": len(candidates), "deleted": deleted, "skipped": skipped}


def _eligible_namespace(namespace: dict[str, Any], cutoff: datetime) -> bool:
    """Whether an S3 Tables namespace is an expired test session's.

    Args:
        namespace: A namespace from ``ListNamespaces`` or ``GetNamespace``.
        cutoff: Namespaces created before this time are expired.

    Returns:
        True for a single-level namespace named like a PyAthena test schema and
        created before ``cutoff``.
    """
    names = namespace.get("namespace") or []
    created = namespace.get("createdAt")
    return (
        len(names) == 1
        and _TEST_NAMESPACE.fullmatch(names[0]) is not None
        and isinstance(created, datetime)
        and created.tzinfo is not None
        and created < cutoff
    )


def sweep_s3tables_namespaces(
    client: Any, table_bucket_arn: str, *, dry_run: bool = True
) -> dict[str, int]:
    """Preview or delete test S3 Tables namespaces older than seven days.

    Test sessions create a namespace named like their schema and delete it when
    they finish; this removes the ones a session left behind. Namespaces younger
    than seven days are retained, including those of running sessions.

    Args:
        client: A boto3 S3 Tables client.
        table_bucket_arn: The ARN of the table bucket to sweep.
        dry_run: Only count eligible namespaces.

    Returns:
        The numbers of eligible, deleted and skipped namespaces.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    # Finish pagination before deleting anything from the table bucket.
    candidates = [
        namespace
        for page in client.get_paginator("list_namespaces").paginate(
            tableBucketARN=table_bucket_arn, prefix="pyathena_test_"
        )
        for namespace in page["namespaces"]
        if _eligible_namespace(namespace, cutoff)
    ]
    deleted = skipped = 0
    for namespace in candidates:
        if dry_run:
            continue
        name = namespace["namespace"][0]
        try:
            current = client.get_namespace(tableBucketARN=table_bucket_arn, namespace=name)
            # Preserve namespaces recreated since listing.
            if (
                not _eligible_namespace(current, cutoff)
                or current["createdAt"] != namespace["createdAt"]
            ):
                skipped += 1
                continue
            tables = [
                table["name"]
                for page in client.get_paginator("list_tables").paginate(
                    tableBucketARN=table_bucket_arn, namespace=name
                )
                for table in page["tables"]
            ]
            for table in tables:
                client.delete_table(tableBucketARN=table_bucket_arn, namespace=name, name=table)
            client.delete_namespace(tableBucketARN=table_bucket_arn, namespace=name)
            deleted += 1
            time.sleep(0.25)
        except client.exceptions.NotFoundException:
            skipped += 1
    return {"eligible": len(candidates), "deleted": deleted, "skipped": skipped}


def main() -> None:
    """Preview or sweep expired test databases and S3 Tables namespaces.

    Pass ``--apply`` to delete; the default only counts. S3 Tables namespaces
    are swept when ``AWS_ATHENA_S3_TABLES_CATALOG`` names a table-bucket
    catalog. The counts are logged and, under GitHub Actions, appended to the
    step summary.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Delete eligible metadata; default: preview"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    config = Config(
        connect_timeout=10,
        read_timeout=30,
        retries={"mode": "standard", "total_max_attempts": 3},
    )
    session = boto3.Session()
    catalog_id = session.client("sts", config=config).get_caller_identity()["Account"]
    mode = "Sweep" if args.apply else "Preview"
    results = {
        "databases": sweep_databases(
            session.client("glue", config=config), catalog_id, dry_run=not args.apply
        )
    }
    s3tables_catalog = os.environ.get("AWS_ATHENA_S3_TABLES_CATALOG")
    if s3tables_catalog:
        bucket = s3tables_catalog.split("/", 1)[1]
        arn = f"arn:aws:s3tables:{session.region_name}:{catalog_id}:bucket/{bucket}"
        results["S3 Tables namespaces"] = sweep_s3tables_namespaces(
            session.client("s3tables", config=config), arn, dry_run=not args.apply
        )
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    for kind, result in results.items():
        summary = (
            f"{mode} {kind}: eligible={result['eligible']}, "
            f"deleted={result['deleted']}, skipped={result['skipped']}"
        )
        _LOGGER.info(summary)
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as output:
                output.write(summary + "\n")


if __name__ == "__main__":
    main()
