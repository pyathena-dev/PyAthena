# Copyright 2020 The PyAthena authors
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

from scripts.sweep_databases import _eligible, main, sweep_databases

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
def test_cli_defaults_to_preview(monkeypatch, tmp_path, arguments, dry_run):
    session = Mock()
    session.client.return_value.get_caller_identity.return_value = {"Account": CATALOG}
    monkeypatch.setattr("scripts.sweep_databases.boto3.Session", lambda: session)
    sweep = Mock(return_value={"eligible": 1, "deleted": int(not dry_run), "skipped": 0})
    monkeypatch.setattr("scripts.sweep_databases.sweep_databases", sweep)
    monkeypatch.setattr("sys.argv", ["sweep_databases.py", *arguments])
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    main()
    sweep.assert_called_once_with(session.client.return_value, CATALOG, dry_run=dry_run)
    assert summary.read_text().startswith("Preview:" if dry_run else "Sweep:")
