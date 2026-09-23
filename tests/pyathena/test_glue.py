# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
from types import SimpleNamespace

import pytest

from pyathena.glue import GlueMetadataCatalog


class TestGlueMetadataCatalog:
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
        assert GlueMetadataCatalog.supports(catalog_name) is expected

    def test_rejects_an_unsupported_catalog(self):
        with pytest.raises(ValueError, match="federated_catalog"):
            GlueMetadataCatalog(SimpleNamespace(), "federated_catalog")

    @staticmethod
    def _client(requests, pages):
        def get_paginator(operation):
            def paginate(**kwargs):
                requests.append((operation, kwargs))
                return pages

            return SimpleNamespace(paginate=paginate)

        def get_table(**kwargs):
            requests.append(("get_table", kwargs))
            return {"Table": {"Name": kwargs["Name"], "StorageDescriptor": {}}}

        return SimpleNamespace(get_paginator=get_paginator, get_table=get_table)

    @pytest.mark.parametrize(
        ("catalog_name", "catalog_kwargs"),
        [
            ("AwsDataCatalog", {}),
            # Glue addresses a table-bucket catalog by its Athena name; the
            # bucket name alone is rejected. Measured live for #786.
            ("s3tablescatalog/bucket", {"CatalogId": "s3tablescatalog/bucket"}),
        ],
    )
    def test_requests_address_the_catalog(self, catalog_name, catalog_kwargs):
        requests = []
        pages = [
            {"TableList": [{"Name": "a"}], "DatabaseList": [{"Name": "db1"}]},
            {"TableList": [{"Name": "b"}], "DatabaseList": [{"Name": "db2"}]},
        ]
        glue = GlueMetadataCatalog(self._client(requests, pages), catalog_name)

        assert glue.get_table("db", "t").name == "t"
        assert [t.name for t in glue.list_tables("db")] == ["a", "b"]
        assert [t.name for t in glue.list_tables("db", "a.*")] == ["a", "b"]
        assert [d.name for d in glue.list_databases()] == ["db1", "db2"]
        assert requests == [
            ("get_table", {"DatabaseName": "db", "Name": "t", **catalog_kwargs}),
            ("get_tables", {"DatabaseName": "db", **catalog_kwargs}),
            ("get_tables", {"DatabaseName": "db", "Expression": "a.*", **catalog_kwargs}),
            ("get_databases", catalog_kwargs),
        ]

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

        metadata = GlueMetadataCatalog.table_metadata(table)

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

        metadata = GlueMetadataCatalog.table_metadata(table)

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
