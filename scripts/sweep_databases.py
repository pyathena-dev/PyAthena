# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Remove expired test database metadata from the caller's default Glue catalog."""

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


def main() -> None:
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
    result = sweep_databases(
        session.client("glue", config=config), catalog_id, dry_run=not args.apply
    )
    mode = "Sweep" if args.apply else "Preview"
    summary = (
        f"{mode}: eligible={result['eligible']}, "
        f"deleted={result['deleted']}, skipped={result['skipped']}"
    )
    _LOGGER.info(summary)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as output:
            output.write(summary + "\n")


if __name__ == "__main__":
    main()
