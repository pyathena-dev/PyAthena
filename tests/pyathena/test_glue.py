# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ParamValidationError

from pyathena.glue import GlueMetadataClient
from tests import ENV
from tests.pyathena.conftest import connect


class TestGlueMetadataClient:
    @pytest.mark.parametrize(
        ("catalog_name", "expected"),
        [
            ("awsdatacatalog", True),
            ("AwsDataCatalog", True),
            ("s3tablescatalog/bucket", True),
            ("federated_catalog", False),
            (None, False),
        ],
    )
    def test_supports(self, catalog_name, expected):
        assert GlueMetadataClient.supports(catalog_name) is expected

    @staticmethod
    def _view(metadata):
        return (
            metadata.name,
            metadata.table_type,
            metadata.create_time,
            [(c.name, c.type, c.comment) for c in metadata.columns],
            [(c.name, c.type, c.comment) for c in metadata.partition_keys],
            metadata.parameters,
        )

    def test_reads_what_athena_reports(self, cursor):
        glue = cursor.connection._glue
        catalog = cursor.connection.catalog_name

        for table in ("one_row", "parquet_with_compression", "partition_table", "view_one_row"):
            assert self._view(glue.get_table(catalog, ENV.schema, table)) == self._view(
                cursor.get_table_metadata(table)
            )
        assert sorted(self._view(m) for m in glue.list_tables(catalog, ENV.schema)) == sorted(
            self._view(m) for m in cursor.list_table_metadata()
        )
        assert [m.name for m in glue.list_tables(catalog, ENV.schema, "one_row")] == ["one_row"]
        assert ENV.schema in [d.name for d in glue.list_databases(catalog)]

    @pytest.mark.skipif(
        not ENV.s3tables_catalog or not ENV.s3tables_namespace,
        reason="AWS_ATHENA_S3_TABLES_CATALOG / AWS_ATHENA_S3_TABLES_NAMESPACE are not configured",
    )
    def test_reads_s3_tables_catalog(self):
        # Glue addresses a table-bucket catalog by its Athena name.
        conn = connect(schema_name=ENV.s3tables_namespace, catalog_name=ENV.s3tables_catalog)
        with conn.cursor() as cursor:
            athena = {(m.name, m.table_type) for m in cursor.list_table_metadata()}
        glue = conn._glue.list_tables(ENV.s3tables_catalog, ENV.s3tables_namespace)

        assert {(m.name, m.table_type) for m in glue} == athena
        assert ENV.s3tables_namespace in [
            d.name for d in conn._glue.list_databases(ENV.s3tables_catalog)
        ]

    def test_reports_a_missing_table(self, cursor):
        with pytest.raises(ClientError) as caught:
            cursor.connection._glue.get_table(
                cursor.connection.catalog_name, ENV.schema, "no_such_table_786"
            )

        assert caught.value.response["Error"]["Code"] == "EntityNotFoundException"
        assert cursor.connection._glue.reachable

    def test_rejects_an_unsupported_catalog(self, cursor):
        with pytest.raises(ValueError, match="federated_catalog"):
            cursor.connection._glue.get_table("federated_catalog", ENV.schema, "one_row")

    def test_stops_after_it_cannot_reach_glue(self, cursor):
        glue = GlueMetadataClient(
            cursor.connection.session,
            "xx-invalid-1",
            Config(connect_timeout=1, read_timeout=1, retries={"max_attempts": 1}),
            {},
        )

        with pytest.raises(EndpointConnectionError):
            glue.get_table("AwsDataCatalog", ENV.schema, "one_row")

        assert not glue.reachable
        assert not glue.usable_for("AwsDataCatalog")

    def test_keeps_going_after_a_request_it_rejects(self, cursor):
        # botocore rejects the request before sending it; Glue stays in use.
        glue = cursor.connection._glue

        with pytest.raises(ParamValidationError):
            glue.get_table(cursor.connection.catalog_name, None, "one_row")

        assert glue.reachable

    def test_client_leaves_out_athena_endpoint(self):
        conn = connect(
            region_name=ENV.region_name,
            endpoint_url=f"https://athena.{ENV.region_name}.amazonaws.com",
            # Athena's API version, which Glue does not have.
            api_version="2017-05-18",
        )
        glue = conn._glue
        # Built once, under the lock, even when cursors in several threads
        # need it together.
        created = []
        session_client = conn.session.client

        def contended_client(*args, **kwargs):
            created.append((args, glue._lock.locked()))
            return session_client(*args, **kwargs)

        conn._session.client = contended_client
        barrier = threading.Barrier(8)

        def get_client(_):
            barrier.wait()
            return glue.client

        with ThreadPoolExecutor(max_workers=8) as executor:
            clients = list(executor.map(get_client, range(8)))

        assert created == [(("glue",), True)]
        assert all(client is clients[0] for client in clients)
        assert clients[0].meta.service_model.service_name == "glue"
        assert clients[0].meta.endpoint_url == f"https://glue.{ENV.region_name}.amazonaws.com"

    def test_table_metadata_leaves_out_columns_iceberg_no_longer_has(self):
        # Glue's columns after DROP COLUMN b and CHANGE COLUMN a a2, as measured
        # for #786; Athena reports a2, c and d.
        def column(name, current):
            return {
                "Name": name,
                "Type": "int",
                "Parameters": {"iceberg.field.current": current, "iceberg.field.id": "1"},
            }

        table = {
            "Name": "t",
            "Parameters": {"table_type": "ICEBERG"},
            "StorageDescriptor": {
                "Columns": [
                    column("a2", "true"),
                    column("c", "true"),
                    column("d", "true"),
                    column("a", "false"),
                    column("b", "false"),
                ]
            },
        }

        metadata = GlueMetadataClient.table_metadata(table)

        assert [c.name for c in metadata.columns] == ["a2", "c", "d"]

    @pytest.mark.parametrize(
        ("table", "expected_parameters"),
        [
            (
                {
                    "Parameters": {"EXTERNAL": "TRUE", "comment": "table comment"},
                    "StorageDescriptor": {
                        "Location": "s3://bucket/hive_text",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": (
                            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
                        ),
                        "SerdeInfo": {
                            "SerializationLibrary": (
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                            ),
                            "Parameters": {"field.delim": "\t"},
                        },
                    },
                },
                {
                    "EXTERNAL": "TRUE",
                    "comment": "table comment",
                    "location": "s3://bucket/hive_text",
                    "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
                    "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "serde.serialization.lib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "serde.param.field.delim": "\t",
                },
            ),
            # A view has empty SerDe information, which Athena still reports.
            (
                {
                    "Parameters": {"comment": "Presto View", "presto_view": "true"},
                    "StorageDescriptor": {"Location": "", "SerdeInfo": {}},
                },
                {
                    "comment": "Presto View",
                    "presto_view": "true",
                    "location": "",
                    "inputformat": None,
                    "outputformat": None,
                    "serde.serialization.lib": None,
                },
            ),
            # An Iceberg table has none, and Athena reports no SerDe library.
            (
                {
                    "Parameters": {"table_type": "ICEBERG", "metadata_location": "s3://m"},
                    "StorageDescriptor": {"Location": "s3://bucket/iceberg"},
                },
                {
                    "table_type": "ICEBERG",
                    "metadata_location": "s3://m",
                    "location": "s3://bucket/iceberg",
                    "inputformat": None,
                    "outputformat": None,
                },
            ),
        ],
        ids=["hive", "view", "iceberg"],
    )
    def test_table_metadata(self, table, expected_parameters):
        # Glue responses measured against GetTableMetadata for the same tables in
        # #786; Athena flattens them this way.
        table = {
            "Name": "t",
            "TableType": "EXTERNAL_TABLE",
            # Athena does not report the Glue description as the comment.
            "Description": "glue description",
            "PartitionKeys": [{"Name": "dt", "Type": "string", "Comment": "day"}],
            **table,
        }
        table["StorageDescriptor"]["Columns"] = [{"Name": "a", "Type": "int"}]

        metadata = GlueMetadataClient.table_metadata(table)

        assert metadata.parameters == expected_parameters
        assert (metadata.name, metadata.table_type, metadata.comment) == (
            "t",
            "EXTERNAL_TABLE",
            expected_parameters.get("comment"),
        )
        assert [(c.name, c.type, c.comment) for c in metadata.columns] == [("a", "int", None)]
        assert [(c.name, c.type, c.comment) for c in metadata.partition_keys] == [
            ("dt", "string", "day")
        ]
