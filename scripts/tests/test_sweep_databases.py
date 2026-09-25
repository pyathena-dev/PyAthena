# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from scripts.sweep_databases import (
    _eligible,
    _eligible_namespace,
    main,
    sweep_databases,
    sweep_s3tables_namespaces,
)

CATALOG = "123456789012"
OLD = datetime.now(timezone.utc) - timedelta(days=10)
DATABASE = {"Name": "pyathena_test_abcdefghij", "CreateTime": OLD}


@pytest.fixture
def glue(monkeypatch):
    session = boto3.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-west-2"
    )
    client = session.client("glue")
    monkeypatch.setattr("scripts.sweep_databases.time.sleep", lambda _: None)
    with Stubber(client) as stubber:
        yield client, stubber
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("pyathena_test_abcdefghij", True),
        ("test_012345abcdef", True),
        ("test_012345abcdef_test_schema", True),
        ("test_012345abcdef_test_schema_2", True),
        ("default", False),
        ("pyathena_benchmark", False),
        ("test_012345ABCDEF", False),
        ("test_012345abcdef_suffix", False),
        ("prefix_pyathena_test_abcdefghij", False),
        ("pyathena_test_abcdefghij_extra", False),
    ],
)
def test_only_fixture_names_are_eligible(name, expected):
    assert _eligible({**DATABASE, "Name": name}, OLD + timedelta(days=1)) is expected


@pytest.mark.parametrize(
    "properties",
    [
        {"CreateTime": None},
        {"CreateTime": OLD.replace(tzinfo=None)},
        {"CreateTime": OLD + timedelta(days=1)},
        {"CreateTime": OLD + timedelta(days=2)},
        {"TargetDatabase": {"CatalogId": CATALOG, "DatabaseName": "shared"}},
        {"FederatedDatabase": {"Identifier": "external", "ConnectionName": "external"}},
    ],
)
def test_unknown_recent_and_reference_databases_are_preserved(properties):
    assert not _eligible({**DATABASE, **properties}, OLD + timedelta(days=1))


def test_preview_and_apply_finish_pagination_before_mutating(glue):
    client, stubber = glue
    args = {"CatalogId": CATALOG}
    target = {**args, "Name": DATABASE["Name"]}
    for dry_run in (True, False):
        stubber.add_response(
            "get_databases", {"DatabaseList": [DATABASE], "NextToken": "next"}, args
        )
        stubber.add_response(
            "get_databases",
            {"DatabaseList": [{**DATABASE, "Name": "default"}]},
            {**args, "NextToken": "next"},
        )
        if not dry_run:
            stubber.add_response("get_database", {"Database": DATABASE}, target)
            stubber.add_response("delete_database", {}, target)
        assert sweep_databases(client, CATALOG, dry_run=dry_run) == {
            "eligible": 1,
            "deleted": int(not dry_run),
            "skipped": 0,
        }


@pytest.mark.parametrize(
    "current",
    [
        {**DATABASE, "CreateTime": OLD - timedelta(days=1)},
        {**DATABASE, "CreateTime": datetime.now(timezone.utc)},
        {**DATABASE, "TargetDatabase": {"CatalogId": CATALOG, "DatabaseName": "shared"}},
    ],
)
def test_database_identity_and_eligibility_are_rechecked(glue, current):
    client, stubber = glue
    stubber.add_response("get_databases", {"DatabaseList": [DATABASE]}, {"CatalogId": CATALOG})
    stubber.add_response(
        "get_database", {"Database": current}, {"CatalogId": CATALOG, "Name": DATABASE["Name"]}
    )
    assert sweep_databases(client, CATALOG, dry_run=False) == {
        "eligible": 1,
        "deleted": 0,
        "skipped": 1,
    }


@pytest.mark.parametrize("operation", ["get_database", "delete_database"])
@pytest.mark.parametrize(
    "error", ["EntityNotFoundException", "AccessDeniedException", "ThrottlingException"]
)
def test_only_concurrent_absence_is_ignored(glue, operation, error):
    client, stubber = glue
    args = {"CatalogId": CATALOG, "Name": DATABASE["Name"]}
    stubber.add_response("get_databases", {"DatabaseList": [DATABASE]}, {"CatalogId": CATALOG})
    if operation == "delete_database":
        stubber.add_response("get_database", {"Database": DATABASE}, args)
    stubber.add_client_error(operation, error, expected_params=args)
    if error == "EntityNotFoundException":
        assert sweep_databases(client, CATALOG, dry_run=False)["skipped"] == 1
    else:
        with pytest.raises(ClientError, match=error):
            sweep_databases(client, CATALOG, dry_run=False)


def test_failed_inventory_does_not_delete_anything(glue):
    client, stubber = glue
    stubber.add_response(
        "get_databases", {"DatabaseList": [DATABASE], "NextToken": "next"}, {"CatalogId": CATALOG}
    )
    stubber.add_client_error(
        "get_databases",
        "ThrottlingException",
        expected_params={"CatalogId": CATALOG, "NextToken": "next"},
    )
    with pytest.raises(ClientError, match="ThrottlingException"):
        sweep_databases(client, CATALOG, dry_run=False)


@pytest.mark.parametrize(("arguments", "dry_run"), [([], True), (["--apply"], False)])
@pytest.mark.parametrize("s3tables_catalog", [None, "s3tablescatalog/table-bucket"])
def test_cli_defaults_to_preview(monkeypatch, tmp_path, arguments, dry_run, s3tables_catalog):
    session = Mock()
    session.client.return_value.get_caller_identity.return_value = {"Account": CATALOG}
    session.client.return_value.meta.region_name = "us-west-2"
    monkeypatch.setattr("scripts.sweep_databases.boto3.Session", lambda: session)
    result = {"eligible": 1, "deleted": int(not dry_run), "skipped": 0}
    sweep = Mock(return_value=result)
    sweep_namespaces = Mock(return_value=result)
    monkeypatch.setattr("scripts.sweep_databases.sweep_databases", sweep)
    monkeypatch.setattr("scripts.sweep_databases.sweep_s3tables_namespaces", sweep_namespaces)
    monkeypatch.setattr("sys.argv", ["sweep_databases.py", *arguments])
    if s3tables_catalog:
        monkeypatch.setenv("AWS_ATHENA_S3_TABLES_CATALOG", s3tables_catalog)
    else:
        monkeypatch.delenv("AWS_ATHENA_S3_TABLES_CATALOG", raising=False)
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    main()
    sweep.assert_called_once_with(session.client.return_value, CATALOG, dry_run=dry_run)
    mode = "Preview" if dry_run else "Sweep"
    lines = summary.read_text().splitlines()
    assert lines[0].startswith(f"{mode} databases:")
    if s3tables_catalog:
        sweep_namespaces.assert_called_once_with(
            session.client.return_value,
            f"arn:aws:s3tables:us-west-2:{CATALOG}:bucket/table-bucket",
            dry_run=dry_run,
        )
        assert lines[1].startswith(f"{mode} S3 Tables namespaces:")
    else:
        sweep_namespaces.assert_not_called()
        assert len(lines) == 1


BUCKET_ARN = f"arn:aws:s3tables:us-west-2:{CATALOG}:bucket/table-bucket"
NAMESPACE = {
    "namespace": ["pyathena_test_abcdefghij"],
    "createdAt": OLD,
    "createdBy": CATALOG,
    "ownerAccountId": CATALOG,
}


@pytest.fixture
def s3tables(monkeypatch):
    session = boto3.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-west-2"
    )
    client = session.client("s3tables")
    monkeypatch.setattr("scripts.sweep_databases.time.sleep", lambda _: None)
    with Stubber(client) as stubber:
        yield client, stubber
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("properties", "expected"),
    [
        ({}, True),
        ({"namespace": ["pyathena"]}, False),
        ({"namespace": ["default"]}, False),
        ({"namespace": ["pyathena_test_abcdefghij_extra"]}, False),
        ({"namespace": ["pyathena_test_ABCDEFGHIJ"]}, False),
        ({"namespace": ["pyathena_test_abcdefghij", "child"]}, False),
        ({"createdAt": OLD + timedelta(days=2)}, False),
        ({"createdAt": OLD.replace(tzinfo=None)}, False),
    ],
)
def test_only_expired_session_namespaces_are_eligible(properties, expected):
    assert _eligible_namespace({**NAMESPACE, **properties}, OLD + timedelta(days=1)) is expected


def test_namespace_sweep_deletes_tables_then_namespace(s3tables):
    client, stubber = s3tables
    listing = {"tableBucketARN": BUCKET_ARN, "prefix": "pyathena_test_"}
    target = {"tableBucketARN": BUCKET_ARN, "namespace": NAMESPACE["namespace"][0]}
    table = {
        "namespace": NAMESPACE["namespace"],
        "name": "leftover",
        "type": "customer",
        "tableARN": f"{BUCKET_ARN}/table/leftover",
        "createdAt": OLD,
        "modifiedAt": OLD,
    }
    for dry_run in (True, False):
        stubber.add_response(
            "list_namespaces", {"namespaces": [NAMESPACE], "continuationToken": "next"}, listing
        )
        stubber.add_response(
            "list_namespaces",
            {"namespaces": [{**NAMESPACE, "namespace": ["pyathena"]}]},
            {**listing, "continuationToken": "next"},
        )
        if not dry_run:
            stubber.add_response("get_namespace", NAMESPACE, target)
            stubber.add_response("list_tables", {"tables": [table]}, target)
            stubber.add_response("delete_table", {}, {**target, "name": "leftover"})
            stubber.add_response("delete_namespace", {}, target)
        assert sweep_s3tables_namespaces(client, BUCKET_ARN, dry_run=dry_run) == {
            "eligible": 1,
            "deleted": int(not dry_run),
            "skipped": 0,
        }


@pytest.mark.parametrize(
    "current",
    [
        {**NAMESPACE, "createdAt": OLD - timedelta(days=1)},
        {**NAMESPACE, "createdAt": datetime.now(timezone.utc)},
    ],
)
def test_namespace_identity_is_rechecked(s3tables, current):
    client, stubber = s3tables
    stubber.add_response(
        "list_namespaces",
        {"namespaces": [NAMESPACE]},
        {"tableBucketARN": BUCKET_ARN, "prefix": "pyathena_test_"},
    )
    stubber.add_response(
        "get_namespace",
        current,
        {"tableBucketARN": BUCKET_ARN, "namespace": NAMESPACE["namespace"][0]},
    )
    assert sweep_s3tables_namespaces(client, BUCKET_ARN, dry_run=False) == {
        "eligible": 1,
        "deleted": 0,
        "skipped": 1,
    }


@pytest.mark.parametrize("error", ["NotFoundException", "AccessDeniedException"])
def test_only_concurrent_namespace_absence_is_ignored(s3tables, error):
    client, stubber = s3tables
    stubber.add_response(
        "list_namespaces",
        {"namespaces": [NAMESPACE]},
        {"tableBucketARN": BUCKET_ARN, "prefix": "pyathena_test_"},
    )
    stubber.add_client_error(
        "get_namespace",
        error,
        expected_params={"tableBucketARN": BUCKET_ARN, "namespace": NAMESPACE["namespace"][0]},
    )
    if error == "NotFoundException":
        assert sweep_s3tables_namespaces(client, BUCKET_ARN, dry_run=False)["skipped"] == 1
    else:
        with pytest.raises(ClientError, match=error):
            sweep_s3tables_namespaces(client, BUCKET_ARN, dry_run=False)


def test_cli_rejects_a_malformed_s3tables_catalog(monkeypatch):
    session = Mock()
    session.client.return_value.get_caller_identity.return_value = {"Account": CATALOG}
    monkeypatch.setattr("scripts.sweep_databases.boto3.Session", lambda: session)
    sweep = Mock()
    monkeypatch.setattr("scripts.sweep_databases.sweep_databases", sweep)
    monkeypatch.setattr("sys.argv", ["sweep_databases.py"])
    monkeypatch.setenv("AWS_ATHENA_S3_TABLES_CATALOG", "table-bucket")
    with pytest.raises(SystemExit):
        main()
    # Nothing is swept when the configuration is wrong.
    sweep.assert_not_called()


def test_cli_reports_databases_before_a_failing_namespace_sweep(monkeypatch, tmp_path):
    session = Mock()
    session.client.return_value.get_caller_identity.return_value = {"Account": CATALOG}
    session.client.return_value.meta.region_name = "us-west-2"
    monkeypatch.setattr("scripts.sweep_databases.boto3.Session", lambda: session)
    monkeypatch.setattr(
        "scripts.sweep_databases.sweep_databases",
        Mock(return_value={"eligible": 2, "deleted": 2, "skipped": 0}),
    )
    monkeypatch.setattr(
        "scripts.sweep_databases.sweep_s3tables_namespaces",
        Mock(side_effect=RuntimeError("namespace sweep failed")),
    )
    monkeypatch.setattr("sys.argv", ["sweep_databases.py", "--apply"])
    monkeypatch.setenv("AWS_ATHENA_S3_TABLES_CATALOG", "s3tablescatalog/table-bucket")
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    with pytest.raises(RuntimeError):
        main()
    assert summary.read_text().splitlines() == ["Sweep databases: eligible=2, deleted=2, skipped=0"]


def test_a_table_already_gone_does_not_stop_the_namespace(s3tables):
    client, stubber = s3tables
    target = {"tableBucketARN": BUCKET_ARN, "namespace": NAMESPACE["namespace"][0]}

    def table(name):
        return {
            "namespace": NAMESPACE["namespace"],
            "name": name,
            "type": "customer",
            "tableARN": f"{BUCKET_ARN}/table/{name}",
            "createdAt": OLD,
            "modifiedAt": OLD,
        }

    stubber.add_response(
        "list_namespaces",
        {"namespaces": [NAMESPACE]},
        {"tableBucketARN": BUCKET_ARN, "prefix": "pyathena_test_"},
    )
    stubber.add_response("get_namespace", NAMESPACE, target)
    stubber.add_response("list_tables", {"tables": [table("gone"), table("left")]}, target)
    stubber.add_client_error(
        "delete_table", "NotFoundException", expected_params={**target, "name": "gone"}
    )
    stubber.add_response("delete_table", {}, {**target, "name": "left"})
    stubber.add_response("delete_namespace", {}, target)
    assert sweep_s3tables_namespaces(client, BUCKET_ARN, dry_run=False) == {
        "eligible": 1,
        "deleted": 1,
        "skipped": 0,
    }
