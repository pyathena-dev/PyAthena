# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import copy
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from pyathena_bench.aws import (
    active_queries,
    cleanup,
    client,
    prepare,
    prepare_object_sets,
    prepare_objects,
    run_locations,
    split_location,
    stack_resources,
    validate_inputs,
    validate_manifest,
    validate_object_sets,
    validate_objects,
)
from pyathena_bench.config import Settings, read_json

RUN = "a" * 32
RESOURCES = {
    "StackId": "arn:stack",
    "Bucket": "temporary",
    "ScratchDatabase": "scratch",
    "SourceBucket": "input",
    "SourcePrefix": "data",
    "SourceDatabase": "pyathena_benchmark",
    "SourceTable": "pypi_file_downloads",
    "WorkGroup": "pyathena",
    "AutoScalingGroup": "fleet-test",
    "Results": "s3://temporary/",
}


def manifest():
    return {
        "version": 1,
        "run_id": RUN,
        "region": "us-west-2",
        "resources": copy.copy(RESOURCES),
        "source": {
            "database": "pyathena_benchmark",
            "table": "pypi_file_downloads",
            "download_date": "2026-09-17",
        },
        "queries": [],
        "scales": {"small": {"table": f"b_{RUN}_small", "rows": 10000, "ready": True}},
    }


@pytest.fixture
def aws_metadata(monkeypatch):
    monkeypatch.setattr("pyathena_bench.aws.session", lambda settings: None)
    monkeypatch.setattr(
        "pyathena_bench.aws.stack_resources", lambda session, stack: copy.copy(RESOURCES)
    )


@pytest.mark.parametrize("change", ["cleaned", "incomplete", "count"])
def test_unusable_snapshots_fail_before_any_measurement(change):
    data = manifest()
    if change == "cleaned":
        data["cleaned"] = True
    elif change == "incomplete":
        data["scales"]["small"]["ready"] = False
    else:
        data["scales"]["small"]["rows"] = 1
    with pytest.raises(ValueError, match=r"Prepare|Prepared"):
        validate_inputs(Settings(), data, ["small"])


class ObjectStore:
    """S3 client fake that keeps the size of each uploaded object."""

    def __init__(self):
        self.sizes = {}

    def upload_fileobj(self, stream, bucket, key):
        data = stream.read()
        assert data == bytes(len(data))
        self.sizes[(bucket, key)] = len(data)

    def head_object(self, **params):
        return {"ContentLength": self.sizes[(params["Bucket"], params["Key"])]}


class TestObjects:
    def test_missing_objects_are_uploaded_once_below_the_run(
        self, aws_metadata, monkeypatch, tmp_path
    ):
        store = ObjectStore()
        monkeypatch.setattr("pyathena_bench.aws.client", lambda session_, service: store)
        settings = Settings(filesystem_sizes={"small": 24 * 2**20, "large": 16 * 2**20})
        data = manifest()
        path = tmp_path / "manifest.json"
        prepare_objects(settings, data, path, ["small"])
        prepare_objects(settings, data, path, ["small", "large"])
        assert sorted(store.sizes.values()) == [16 * 2**20, 24 * 2**20]
        assert read_json(path)["objects"] == data["objects"]
        location = data["objects"]["small"]["location"]
        assert location.startswith(f"s3://temporary/runs/{RUN}/objects/small-")
        assert data["objects"]["small"]["size"] == 24 * 2**20
        validate_objects(settings, data, ["small", "large"])

    @pytest.mark.parametrize(
        ("change", "message"),
        [("missing", "Prepare"), ("cleaned", "Prepare"), ("size", "Prepared object size")],
    )
    def test_unusable_objects_fail_before_any_measurement(self, change, message):
        data = manifest()
        data["objects"] = {"small": {"location": "s3://temporary/o", "size": 64 * 2**20}}
        scales = ["small"]
        if change == "missing":
            scales = ["small", "large"]
        elif change == "cleaned":
            data["cleaned"] = True
        else:
            data["objects"]["small"]["size"] = 1
        with pytest.raises(ValueError, match=message):
            validate_objects(Settings(filesystem_sizes={"small": 64 * 2**20}), data, scales)


class ObjectSetStore(ObjectStore):
    """Adds copies, empty puts, and listing by prefix to the object store."""

    def __init__(self):
        super().__init__()
        self.requests = []

    def copy_object(self, **params):
        self.requests.append("copy")
        source = params["CopySource"]
        assert params["TaggingDirective"] == "REPLACE"
        assert params["AnnotationDirective"] == "EXCLUDE"
        self.sizes[(params["Bucket"], params["Key"])] = self.sizes[
            (source["Bucket"], source["Key"])
        ]

    def put_object(self, **params):
        self.requests.append("put")
        assert params["Body"] == b""
        self.sizes[(params["Bucket"], params["Key"])] = 0

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **params):
        contents = [
            {"Key": key, "Size": size}
            for (bucket, key), size in sorted(self.sizes.items())
            if bucket == params["Bucket"] and key.startswith(params["Prefix"])
        ]
        return [{"Contents": contents[:2]}, {"Contents": contents[2:]}]


class TestObjectSets:
    def test_missing_sets_are_uploaded_once_as_copies_and_empty_keys(
        self, aws_metadata, monkeypatch, tmp_path
    ):
        store = ObjectSetStore()
        monkeypatch.setattr("pyathena_bench.aws.client", lambda session_, service: store)
        settings = Settings(
            objects_counts={"small": 3, "large": 2},
            objects_size=16,
            objects_listing_keys_per_object=2,
        )
        data = manifest()
        path = tmp_path / "manifest.json"
        prepare_object_sets(settings, data, path, ["small"])
        prepare_object_sets(settings, data, path, ["small", "large"])
        # One upload per set; the other files are copies; the listing keys are empty puts.
        assert store.requests.count("copy") == 2 + 1
        assert store.requests.count("put") == 3 * 2 + 2 * 2
        small = data["object_sets"]["small"]
        assert small["files"].startswith(f"s3://temporary/runs/{RUN}/object-sets/small-")
        assert small["listing"] == small["files"].replace("files/", "listing/")
        assert (small["count"], small["size"], small["keys"]) == (3, 16, 6)
        assert store.sizes[("temporary", small["files"].split("temporary/")[1] + "000002")] == 16
        assert (
            store.sizes[("temporary", small["listing"].split("temporary/")[1] + "000002/001")] == 0
        )
        assert read_json(path)["object_sets"] == data["object_sets"]
        validate_object_sets(settings, data, ["small", "large"])
        validate_manifest(settings, data)

    @pytest.mark.parametrize(
        ("change", "message"),
        [("missing", "Prepare"), ("cleaned", "Prepare"), ("count", "Prepared object set")],
    )
    def test_unusable_sets_fail_before_any_measurement(self, change, message):
        data = manifest()
        data["object_sets"] = {
            "small": {
                "files": "s3://t/f/",
                "listing": "s3://t/l/",
                "count": 2,
                "size": 16,
                "keys": 4,
            }
        }
        scales = ["small"]
        if change == "missing":
            scales = ["small", "large"]
        elif change == "cleaned":
            data["cleaned"] = True
        else:
            data["object_sets"]["small"]["count"] = 1
        settings = Settings(
            objects_counts={"small": 2}, objects_size=16, objects_listing_keys_per_object=2
        )
        with pytest.raises(ValueError, match=message):
            validate_object_sets(settings, data, scales)

    def test_manifest_rejects_a_set_outside_the_run(self, aws_metadata):
        data = manifest()
        data["object_sets"] = {
            "small": {
                "files": f"s3://temporary/runs/{RUN}/object-sets/small-x/files/",
                "listing": "s3://temporary/elsewhere/listing/",
                "count": 2,
                "size": 16,
                "keys": 4,
            }
        }
        with pytest.raises(ValueError, match="Object set is outside"):
            validate_manifest(Settings(), data)


class TestAwsLifecycle:
    @pytest.mark.parametrize("change", ["bucket", "table", "object", "region", "date"])
    def test_manifest_rejects_changed_or_foreign_targets(self, aws_metadata, change):
        data = manifest()
        data["objects"] = {
            "small": {"location": f"s3://temporary/runs/{RUN}/objects/small-x", "size": 1}
        }
        validate_manifest(Settings(), data)
        if change == "bucket":
            data["resources"]["Bucket"] = "input"
        elif change == "table":
            data["scales"]["small"]["table"] = "pypi_file_downloads"
        elif change == "object":
            data["objects"]["small"]["location"] = "s3://input/data/part-0"
        elif change == "region":
            data["region"] = "us-east-1"
        else:
            data["source"]["download_date"] = "2026-09-18"
        with pytest.raises(ValueError, match=r"changed|outside|mismatch"):
            validate_manifest(Settings(), data)

    def test_failed_preparation_retains_cleanup_manifest(self, monkeypatch, tmp_path):
        monkeypatch.setattr("pyathena_bench.aws.preflight", lambda settings, stack: RESOURCES)
        path = tmp_path / "input.json"

        def fail(settings, resources, data, saved, sql, output):
            assert read_json(saved)["scales"]["small"]["ready"] is False
            assert "download_date = '2026-09-17' LIMIT 10000" in sql
            assert "write_compression='SNAPPY'" in sql
            raise RuntimeError("CTAS failed")

        monkeypatch.setattr("pyathena_bench.aws.execute_query", fail)
        with pytest.raises(RuntimeError, match="CTAS failed"):
            prepare(Settings(), "stack", path, ["small"])
        assert read_json(path)["scales"]["small"]["location"].startswith("s3://temporary/runs/")

    def test_preparation_without_scales_records_only_the_stack(self, monkeypatch, tmp_path):
        monkeypatch.setattr("pyathena_bench.aws.preflight", lambda settings, stack: RESOURCES)
        monkeypatch.setattr(
            "pyathena_bench.aws.execute_query",
            lambda *args: pytest.fail("No snapshot is created without scales"),
        )
        path = tmp_path / "input.json"
        prepare(Settings(), "stack", path, [])
        assert read_json(path)["resources"] == RESOURCES
        assert read_json(path)["scales"] == {}


class TestCleanup:
    def test_cleanup_rejects_a_foreign_query_before_cancelling(
        self, aws_metadata, monkeypatch, tmp_path
    ):
        data = manifest()
        data["queries"] = [{"query_id": "foreign"}]
        fake = SimpleNamespace(
            get_query_execution=lambda **kwargs: {
                "QueryExecution": {
                    "WorkGroup": "pyathena",
                    "ResultConfiguration": {"OutputLocation": "s3://input/data"},
                }
            }
        )
        monkeypatch.setattr("pyathena_bench.aws.client", lambda *args: fake)
        with pytest.raises(ValueError, match="query outside this run"):
            cleanup(Settings(), data, tmp_path / "manifest.json")

    def test_cleanup_deletes_only_run_owned_tables_and_prefix(
        self, aws_metadata, monkeypatch, tmp_path
    ):
        actions = []

        class Fake:
            def get_paginator(self, operation):
                def pages(**kwargs):
                    actions.append((operation, kwargs))
                    return {
                        "list_query_executions": [{"QueryExecutionIds": []}],
                        "get_tables": [
                            {"TableList": [{"Name": f"b_{RUN}_small"}, {"Name": "foreign"}]}
                        ],
                        "list_multipart_uploads": [
                            {"Uploads": [{"Key": f"runs/{RUN}/part", "UploadId": "id"}]}
                        ],
                        "list_objects_v2": [{"Contents": [{"Key": f"runs/{RUN}/data"}]}],
                    }[operation]

                return SimpleNamespace(paginate=pages)

            def delete_table(self, **kwargs):
                actions.append(("delete_table", kwargs))

            def abort_multipart_upload(self, **kwargs):
                actions.append(("abort", kwargs))

            def delete_objects(self, **kwargs):
                actions.append(("delete_objects", kwargs))
                return {}

        monkeypatch.setattr("pyathena_bench.aws.client", lambda session, service: Fake())
        monkeypatch.setattr("pyathena_bench.aws.cancel_queries", lambda settings, ids: [])
        cleanup(Settings(), manifest(), tmp_path / "manifest.json")
        assert ("delete_table", {"DatabaseName": "scratch", "Name": f"b_{RUN}_small"}) in actions
        assert all(kwargs.get("Bucket", "temporary") == "temporary" for _, kwargs in actions)
        assert all(kwargs.get("Name") != "foreign" for _, kwargs in actions)
        assert read_json(tmp_path / "manifest.json")["cleaned"]

    @pytest.mark.parametrize("trials_only", [False, True])
    def test_cleanup_batches_history_and_can_preserve_snapshots(
        self, aws_metadata, monkeypatch, tmp_path, trials_only
    ):
        actions = []
        trial_table = f"b_{RUN}_{'b' * 32}"
        root = f"runs/{RUN}/"

        class Fake:
            def get_paginator(self, operation):
                def pages(**kwargs):
                    actions.append((operation, kwargs))
                    return {
                        "list_query_executions": [
                            {"QueryExecutionIds": [str(i) for i in range(51)]}
                        ],
                        "get_tables": [
                            {
                                "TableList": [
                                    {"Name": f"b_{RUN}_small"},
                                    {"Name": trial_table},
                                    {"Name": "foreign"},
                                ]
                            }
                        ],
                        "list_multipart_uploads": [{}],
                        "list_objects_v2": [{}],
                    }[operation]

                return SimpleNamespace(paginate=pages)

            def get_query_execution(self, **kwargs):
                return {"QueryExecution": {"Status": {"State": "CANCELLED"}}}

            def batch_get_query_execution(self, **kwargs):
                query_ids = kwargs["QueryExecutionIds"]
                actions.append(("batch", query_ids))
                return {
                    "QueryExecutions": [
                        {
                            "QueryExecutionId": q,
                            "WorkGroup": "pyathena",
                            "Status": {
                                "State": "RUNNING" if q in {"0", "1", "2", "3"} else "SUCCEEDED"
                            },
                            "ResultConfiguration": {
                                "OutputLocation": "s3://temporary/"
                                + (
                                    root + "trials/t/"
                                    if q == "0"
                                    else root + "prepare/"
                                    if q == "1"
                                    else root + "fixtures/f/"
                                    if q == "3"
                                    else "foreign/"
                                )
                            },
                        }
                        for q in query_ids
                    ]
                }

            def delete_table(self, **kwargs):
                actions.append(("delete_table", kwargs["Name"]))

        monkeypatch.setattr("pyathena_bench.aws.client", lambda *args: Fake())
        monkeypatch.setattr(
            "pyathena_bench.aws.cancel_queries",
            lambda settings, ids: actions.append(("cancel", ids)) or [],
        )
        data = manifest()
        cleanup(Settings(), data, tmp_path / "manifest.json", trials_only=trials_only)
        assert [len(value) for action, value in actions if action == "batch"] == [50, 1]
        assert ("cancel", {"0", "1", "3"}) in actions
        assert ("delete_table", trial_table) in actions
        assert (("delete_table", f"b_{RUN}_small") in actions) is not trials_only
        assert ("delete_table", "foreign") not in actions
        assert data.get("cleaned", False) is not trials_only
        assert all(
            value["Prefix"] == root + ("trials/" if trials_only else "")
            for action, value in actions
            if action in {"list_objects_v2", "list_multipart_uploads"}
        )

    def test_cleanup_aborts_before_deletion_if_history_batch_is_incomplete(
        self, aws_metadata, monkeypatch, tmp_path
    ):
        fake = SimpleNamespace(
            get_paginator=lambda operation: SimpleNamespace(
                paginate=lambda **kwargs: [{"QueryExecutionIds": ["q"]}]
            ),
            batch_get_query_execution=lambda **kwargs: {
                "UnprocessedQueryExecutionIds": [
                    {"QueryExecutionId": "q", "ErrorCode": "InternalServerException"}
                ]
            },
        )
        monkeypatch.setattr("pyathena_bench.aws.client", lambda *args: fake)
        with pytest.raises(RuntimeError, match="Could not inspect query history"):
            cleanup(Settings(), manifest(), tmp_path / "manifest.json")

    @pytest.mark.parametrize("error_code", ["InvalidRequestException", "AccessDeniedException"])
    def test_cleanup_missing_history_does_not_hide_access_errors(
        self, aws_metadata, monkeypatch, tmp_path, error_code
    ):
        data = manifest()
        data["queries"] = [{"query_id": "expired"}]

        def unavailable(**kwargs):
            raise ClientError(
                {"Error": {"Code": error_code, "Message": "Unavailable"}}, "GetQueryExecution"
            )

        def paginator(operation):
            if operation == "list_query_executions":
                raise RuntimeError("live history reached")
            pytest.fail("Deletion attempted before live history check")

        monkeypatch.setattr(
            "pyathena_bench.aws.client",
            lambda *args: SimpleNamespace(get_query_execution=unavailable, get_paginator=paginator),
        )
        if error_code == "InvalidRequestException":
            with (
                pytest.warns(UserWarning, match="metadata unavailable"),
                pytest.raises(RuntimeError, match="live history reached"),
            ):
                cleanup(Settings(), data, tmp_path / "manifest.json")
        else:
            with pytest.raises(ClientError, match="AccessDeniedException"):
                cleanup(Settings(), data, tmp_path / "manifest.json")


def test_cleanup_covers_a_separate_results_location(aws_metadata, monkeypatch, tmp_path):
    resources = {**RESOURCES, "Results": "s3://results/bench/stack/"}
    monkeypatch.setattr(
        "pyathena_bench.aws.stack_resources", lambda session, stack: dict(resources)
    )
    actions = []
    scanned = {}

    class Fake:
        def get_paginator(self, operation):
            def pages(**kwargs):
                actions.append((operation, kwargs))
                return {
                    "get_tables": [{"TableList": []}],
                    "list_multipart_uploads": [{}],
                    "list_objects_v2": [{}],
                }[operation]

            return SimpleNamespace(paginate=pages)

    def active(athena, workgroup, prefixes):
        scanned["prefixes"] = prefixes
        return set()

    monkeypatch.setattr("pyathena_bench.aws.client", lambda *args: Fake())
    monkeypatch.setattr("pyathena_bench.aws.active_queries", active)
    monkeypatch.setattr("pyathena_bench.aws.cancel_queries", lambda settings, ids: [])
    data = manifest()
    data["resources"] = dict(resources)
    cleanup(Settings(), data, tmp_path / "manifest.json", trials_only=True)
    assert scanned["prefixes"] == (
        f"s3://results/bench/stack/runs/{RUN}/",
        f"s3://temporary/runs/{RUN}/",
    )
    listed = {(k["Bucket"], k["Prefix"]) for op, k in actions if op == "list_objects_v2"}
    assert listed == {
        ("results", f"bench/stack/runs/{RUN}/trials/"),
        ("temporary", f"runs/{RUN}/trials/"),
    }


def test_active_queries_scans_past_unprocessed_ids_before_raising():
    pages = [{"QueryExecutionIds": ["match", "x"]}, {"QueryExecutionIds": ["later"]}]

    def running(query_id):
        return {
            "QueryExecutionId": query_id,
            "WorkGroup": "pyathena",
            "ResultConfiguration": {"OutputLocation": f"s3://b/trials/t1/{query_id}"},
            "Status": {"State": "RUNNING"},
        }

    def batch_get_query_execution(**kwargs):
        if kwargs["QueryExecutionIds"] == ["match", "x"]:
            return {
                "QueryExecutions": [running("match")],
                "UnprocessedQueryExecutionIds": [{"QueryExecutionId": "x"}],
            }
        return {"QueryExecutions": [running("later")]}

    athena = SimpleNamespace(
        get_paginator=lambda name: SimpleNamespace(paginate=lambda **kwargs: pages),
        batch_get_query_execution=batch_get_query_execution,
    )
    found = set()
    with pytest.raises(RuntimeError, match=r"Could not inspect query history: .*'x'"):
        active_queries(athena, "pyathena", ("s3://b/trials/t1/",), found)
    assert found == {"match", "later"}


def test_active_queries_keeps_unprocessed_ids_when_a_later_request_fails():
    pages = [{"QueryExecutionIds": ["x"]}, {"QueryExecutionIds": ["later"]}]

    def batch_get_query_execution(**kwargs):
        if kwargs["QueryExecutionIds"] == ["x"]:
            return {
                "QueryExecutions": [],
                "UnprocessedQueryExecutionIds": [{"QueryExecutionId": "x"}],
            }
        raise ClientError({"Error": {"Code": "ThrottlingException"}}, "BatchGetQueryExecution")

    athena = SimpleNamespace(
        get_paginator=lambda name: SimpleNamespace(paginate=lambda **kwargs: pages),
        batch_get_query_execution=batch_get_query_execution,
    )
    # A throttling error must not hide the IDs that could not be inspected.
    with pytest.raises(RuntimeError, match=r"'x'.*; then ClientError: .*ThrottlingException"):
        active_queries(athena, "pyathena", ("s3://b/trials/t1/",))


@pytest.mark.parametrize(("kwargs", "retries"), [({}, 5), ({"max_retries": 10}, 10)])
def test_client_retries_in_standard_mode(kwargs, retries):
    created = []
    session_ = SimpleNamespace(client=lambda service, config: created.append(config) or service)
    assert client(session_, "athena", **kwargs) == "athena"
    assert created[0].retries == {"mode": "standard", "max_attempts": retries}


@pytest.mark.parametrize(
    ("location", "expected"),
    [("s3://bucket/", ("bucket", "")), ("s3://bucket/a/b/", ("bucket", "a/b/"))],
)
def test_split_location(location, expected):
    assert split_location(location) == expected


@pytest.mark.parametrize("location", ["s3://bucket", "s3://bucket/a", "bucket/a/", "s3://B/"])
def test_split_location_rejects_invalid_locations(location):
    with pytest.raises(ValueError, match="Invalid S3 location"):
        split_location(location)


def test_stack_resources_require_the_results_output(monkeypatch):
    outputs = {k: v for k, v in RESOURCES.items() if k != "StackId"}
    stack = {
        "StackId": "arn:stack",
        "Tags": [{"Key": "Purpose", "Value": "pyathena-benchmark"}],
        "Outputs": [{"OutputKey": k, "OutputValue": v} for k, v in outputs.items()],
    }
    fake = SimpleNamespace(describe_stacks=lambda **kwargs: {"Stacks": [stack]})
    monkeypatch.setattr("pyathena_bench.aws.client", lambda *args: fake)
    assert stack_resources(None, "stack")["Results"] == "s3://temporary/"
    stack["Outputs"] = [o for o in stack["Outputs"] if o["OutputKey"] != "Results"]
    with pytest.raises(ValueError, match="Stack outputs are incomplete"):
        stack_resources(None, "stack")


@pytest.mark.parametrize(
    ("results", "expected"),
    [
        ("s3://temporary/", f"s3://temporary/runs/{RUN}/"),
        ("s3://results/bench/stack/", f"s3://results/bench/stack/runs/{RUN}/"),
    ],
)
def test_run_locations(results, expected):
    locations = run_locations({**RESOURCES, "Results": results}, RUN)
    assert locations == {"scratch": f"s3://temporary/runs/{RUN}/", "results": expected}
