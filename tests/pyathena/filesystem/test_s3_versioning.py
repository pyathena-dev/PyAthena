# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

"""Opt-in real S3 tests of null-version moves using temporary buckets."""

import os
import time
import uuid

import boto3
import pytest

from pyathena.filesystem.s3 import S3FileSystem
from pyathena.filesystem.s3_async import AioS3FileSystem
from tests import ENV

pytestmark = pytest.mark.skipif(
    os.getenv("AWS_ATHENA_S3_VERSIONING_TESTS") != "1",
    reason="Set AWS_ATHENA_S3_VERSIONING_TESTS=1 to create temporary versioning test buckets.",
)

BACKENDS = ["sync", "async", "async-wrapper"]


@pytest.fixture(scope="module")
def versioning_buckets():
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
            for key in BACKENDS:
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
            for key in BACKENDS:
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


@pytest.fixture(params=BACKENDS)
def fs(request):
    backend = request.param
    cls = S3FileSystem if backend == "sync" else AioS3FileSystem
    return backend, cls(region_name=ENV.region_name, skip_instance_cache=True)


class TestS3NullVersionMove:
    @pytest.mark.parametrize("status", [None, "Enabled", "Suspended"])
    @pytest.mark.asyncio
    async def test_mv_null_version_onto_key(self, fs, versioning_buckets, status):
        backend, filesystem = fs
        client, buckets = versioning_buckets
        bucket = buckets[status]
        path = f"s3://{bucket}/{backend}"
        before = [
            v
            for v in client.list_object_versions(Bucket=bucket, Prefix=backend)["Versions"]
            if v["Key"] == backend
        ]
        assert any(v["VersionId"] == "null" for v in before)
        if status:
            assert not next(v for v in before if v["VersionId"] == "null")["IsLatest"]

        if backend == "async":
            await filesystem._mv(f"{path}?versionId=null", path)
        else:
            filesystem.mv(f"{path}?versionId=null", path)

        with client.get_object(Bucket=bucket, Key=backend)["Body"] as body:
            assert body.read() == (b"original" if status != "Suspended" else b"current")
        after = [
            v
            for v in client.list_object_versions(Bucket=bucket, Prefix=backend)["Versions"]
            if v["Key"] == backend
        ]
        if status == "Enabled":
            assert not any(v["VersionId"] == "null" for v in after)
            assert len(after) == len(before)
            latest = next(v for v in after if v["IsLatest"])
            assert latest["VersionId"] not in {v["VersionId"] for v in before}
        else:
            assert after == before
