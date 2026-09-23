# Copyright 2022 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from pathlib import Path

from botocore.exceptions import ClientError
from jinja2 import Environment, FileSystemLoader

_queries = Environment(
    loader=FileSystemLoader(Path(__file__).parents[1].resolve() / "resources" / "queries")
)


def read_query(name, **kwargs):
    template = _queries.get_template(name)
    return [q.strip() for q in template.render(**kwargs).split(";") if q and q.strip()]


METADATA_OPERATIONS = ("get_table_metadata", "list_table_metadata", "list_databases")


def throttle_metadata_api(
    client,
    monkeypatch,
    code="ThrottlingException",
    message="Rate exceeded",
    operations=METADATA_OPERATIONS,
):
    """Make the Athena client's metadata requests fail with ``code``; return the call list."""
    calls = []

    def failing(operation):
        def fail(**kwargs):
            calls.append(operation)
            raise ClientError({"Error": {"Code": code, "Message": message}}, operation)

        return fail

    for operation in operations:
        monkeypatch.setattr(client, operation, failing(operation))
    return calls
