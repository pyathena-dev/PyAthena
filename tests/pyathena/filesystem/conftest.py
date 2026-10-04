# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Shared S3 filesystem integration-test fixtures."""

import os
import time
import uuid

import boto3
import pytest

from tests import ENV

VERSIONING_TEST_KEYS = ("sync", "async", "async-wrapper")


@pytest.fixture(scope="session")
def versioning_buckets():
    """Share temporary buckets across the sync and async opt-in move tests."""
    if os.getenv("AWS_ATHENA_S3_VERSIONING_TESTS") != "1":
        pytest.skip("Set AWS_ATHENA_S3_VERSIONING_TESTS=1 to create versioning test buckets.")
    client = boto3.client("s3", region_name=ENV.region_name)
    buckets = {}
    try:
        for status in (None, "Enabled", "Suspended"):
            bucket = f"pyathena-mv-null-{uuid.uuid4().hex}"
            params = (
                {"CreateBucketConfiguration": {"LocationConstraint": ENV.region_name}}
                if ENV.region_name != "us-east-1"
                else {}
            )
            client.create_bucket(Bucket=bucket, **params)
            buckets[status] = bucket
            for key in VERSIONING_TEST_KEYS:
                client.put_object(Bucket=bucket, Key=key, Body=b"original")
            if status:
                client.put_bucket_versioning(
                    Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
                )

        # AWS recommends waiting 15 minutes after first enabling versioning
        # before writing. Both versioned buckets propagate during this wait.
        deadline = time.monotonic() + 15 * 60
        while (remaining := deadline - time.monotonic()) > 0:
            time.sleep(min(30, remaining))
        for status in ("Enabled", "Suspended"):
            bucket = buckets[status]
            for key in VERSIONING_TEST_KEYS:
                response = client.put_object(Bucket=bucket, Key=key, Body=b"current")
                assert response["VersionId"] != "null"
            if status == "Suspended":
                client.put_bucket_versioning(
                    Bucket=bucket, VersioningConfiguration={"Status": "Suspended"}
                )
        yield client, buckets
    finally:
        # A failure to clean one bucket must not strand the other buckets.
        errors = []
        for bucket in buckets.values():
            try:
                objects = [
                    {"Key": version["Key"], "VersionId": version["VersionId"]}
                    for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket)
                    for version in page.get("Versions", []) + page.get("DeleteMarkers", [])
                ]
                for start in range(0, len(objects), 1000):
                    response = client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": objects[start : start + 1000], "Quiet": True},
                    )
                    if response.get("Errors"):
                        raise OSError(f"Failed to clean {bucket}: {response['Errors']}")
                client.delete_bucket(Bucket=bucket)
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("Failed to remove versioning test buckets", errors)
