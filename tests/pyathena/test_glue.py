# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from botocore.exceptions import ParamValidationError

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

    @pytest.mark.skipif(
        not ENV.s3tables_catalog,
        reason="AWS_ATHENA_S3_TABLES_CATALOG is not configured",
    )
    def test_reads_s3_tables_catalog(self):
        # A table of its own, so a rerun of this test starts clean. Listing the
        # bucket's namespaces is left out: other sessions create and delete
        # theirs, and a listing fails when a namespace goes away during it.
        schema = ENV.s3tables_namespace
        table = f"test_glue_reads_s3_tables_{uuid.uuid4().hex[:8]}"
        with (
            connect(schema_name=schema, catalog_name=ENV.s3tables_catalog) as conn,
            conn.cursor() as cursor,
        ):
            try:
                # Inside try: a CREATE that fails after Athena made the table
                # still gets it dropped.
                cursor.execute(
                    f"CREATE TABLE {schema}.{table} (a INT, b STRING) "
                    "PARTITIONED BY (b) TBLPROPERTIES ('table_type'='ICEBERG')"
                )
                glue = conn._glue
                # Glue addresses a table-bucket catalog by its Athena name.
                assert self._view(
                    glue.get_table(ENV.s3tables_catalog, schema, table)
                ) == self._view(cursor.get_table_metadata(table))
                listed = glue.list_tables(ENV.s3tables_catalog, schema, table)
                assert [m.name for m in listed] == [table]
                assert [self._view(m) for m in listed] == [
                    self._view(m) for m in cursor.list_table_metadata(expression=table)
                ]
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

    def test_keeps_going_after_a_request_it_rejects(self, cursor):
        # botocore rejects the request before sending it; Glue stays in use.
        glue = cursor.connection._glue

        with pytest.raises(ParamValidationError):
            glue.get_table(cursor.connection.catalog_name, None, "one_row")

        assert glue.reachable

    def test_client_leaves_out_athena_endpoint(self):
        with connect(
            region_name=ENV.region_name,
            endpoint_url=f"https://athena.{ENV.region_name}.amazonaws.com",
            # Athena's API version, which Glue does not have.
            api_version="2017-05-18",
        ) as conn:
            glue = conn._glue
            created = []
            session_client = conn.session.client

            def contended_client(*args, **kwargs):
                created.append(args)
                return session_client(*args, **kwargs)

            conn.session.client = contended_client
            barrier = threading.Barrier(8)

            def get_client(_):
                barrier.wait()
                return glue.client

            with ThreadPoolExecutor(max_workers=8) as executor:
                clients = list(executor.map(get_client, range(8)))

            assert created == [("glue",)]
            assert all(client is clients[0] for client in clients)
            assert clients[0].meta.service_model.service_name == "glue"
            assert clients[0].meta.service_model.api_version == "2017-03-31"
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
            # A view has no formats and empty SerDe information; Athena reports
            # only its empty location.
            (
                {
                    "Parameters": {"comment": "Presto View", "presto_view": "true"},
                    "StorageDescriptor": {"Location": "", "SerdeInfo": {}},
                },
                {
                    "comment": "Presto View",
                    "presto_view": "true",
                    "location": "",
                },
            ),
            # An Iceberg table has no formats and no SerDe information.
            (
                {
                    "Parameters": {"table_type": "ICEBERG", "metadata_location": "s3://m"},
                    "StorageDescriptor": {"Location": "s3://bucket/iceberg"},
                },
                {
                    "table_type": "ICEBERG",
                    "metadata_location": "s3://m",
                    "location": "s3://bucket/iceberg",
                },
            ),
            # An S3 Tables table has empty formats, which Athena reports.
            (
                {
                    "Parameters": {"table_type": "ICEBERG"},
                    "StorageDescriptor": {
                        "Location": "s3://bucket--table-s3",
                        "InputFormat": "",
                        "OutputFormat": "",
                    },
                },
                {
                    "table_type": "ICEBERG",
                    "location": "s3://bucket--table-s3",
                    "inputformat": "",
                    "outputformat": "",
                },
            ),
            # The descriptor replaces table parameters of the same name, also
            # when it leaves the value unset.
            (
                {
                    "Parameters": {
                        "location": "s3://table-parameter",
                        "inputformat": "TableParameterInputFormat",
                        "serde.serialization.lib": "TableParameterSerDe",
                    },
                    "StorageDescriptor": {"Location": "", "SerdeInfo": {}},
                },
                {"location": ""},
            ),
        ],
        ids=["hive", "view", "iceberg", "s3_tables", "table_parameter_collision"],
    )
    def test_table_metadata(self, table, expected_parameters):
        # Glue responses measured against GetTableMetadata for the same tables in
        # #786 and #887; Athena flattens them this way.
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
