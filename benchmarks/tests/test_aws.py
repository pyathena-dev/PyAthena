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
from pyathena_bench.aws import cleanup, prepare, validate_inputs, validate_manifest
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


class TestAwsLifecycle:
    @pytest.mark.parametrize("change", ["bucket", "table", "region", "date"])
    def test_manifest_rejects_changed_or_foreign_targets(self, aws_metadata, change):
        data = manifest()
        if change == "bucket":
            data["resources"]["Bucket"] = "input"
        elif change == "table":
            data["scales"]["small"]["table"] = "pypi_file_downloads"
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
