# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
"""Read Athena table and database metadata from the AWS Glue Data Catalog."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pyathena.model import AthenaDatabase, AthenaTableMetadata


class GlueMetadataCatalog:
    """A Glue-backed Athena data catalog, read through the Glue API.

    Reports the metadata Athena's ``GetTableMetadata``, ``ListTableMetadata``
    and ``ListDatabases`` would give for the same catalog, from ``GetTable``,
    ``GetTables`` and ``GetDatabases``.

    Args:
        client: A boto3 Glue client.
        catalog_name: An Athena catalog name that :meth:`supports` accepts.

    Raises:
        ValueError: If Glue cannot answer for the catalog.
    """

    def __init__(self, client: Any, catalog_name: str | None) -> None:
        request_kwargs = self._catalog_request_kwargs(catalog_name)
        if request_kwargs is None:
            raise ValueError(f"Glue cannot answer for the catalog {catalog_name!r}.")
        self._client = client
        self._request_kwargs = request_kwargs

    @staticmethod
    def _catalog_request_kwargs(catalog_name: str | None) -> dict[str, str] | None:
        # AwsDataCatalog is the caller's default Glue catalog. An S3 Tables
        # catalog is a Glue federated catalog addressed by its Athena name.
        lowered = (catalog_name or "").lower()
        if lowered == "awsdatacatalog":
            return {}
        if lowered.startswith("s3tablescatalog/"):
            return {"CatalogId": catalog_name}  # type: ignore[dict-item]
        return None

    @classmethod
    def supports(cls, catalog_name: str | None) -> bool:
        """Whether the Athena catalog is one Glue can answer for.

        Args:
            catalog_name: An Athena catalog name.

        Returns:
            True for ``AwsDataCatalog`` and S3 Tables catalogs
            (``s3tablescatalog/<table-bucket>``).
        """
        return cls._catalog_request_kwargs(catalog_name) is not None

    def get_table(self, schema_name: str | None, table_name: str) -> AthenaTableMetadata:
        """Get one table's metadata with ``GetTable``."""
        response = self._client.get_table(
            DatabaseName=schema_name, Name=table_name, **self._request_kwargs
        )
        return self.table_metadata(response["Table"])

    def list_tables(
        self, schema_name: str | None, expression: str | None = None
    ) -> list[AthenaTableMetadata]:
        """List a database's table metadata with ``GetTables``."""
        request: dict[str, Any] = {"DatabaseName": schema_name, **self._request_kwargs}
        if expression:
            request["Expression"] = expression
        pages = self._client.get_paginator("get_tables").paginate(**request)
        return [self.table_metadata(t) for page in pages for t in page["TableList"]]

    def list_databases(self) -> list[AthenaDatabase]:
        """List the catalog's databases with ``GetDatabases``."""
        pages = self._client.get_paginator("get_databases").paginate(**self._request_kwargs)
        return [AthenaDatabase({"Database": d}) for page in pages for d in page["DatabaseList"]]

    @staticmethod
    def table_metadata(table: Mapping[str, Any]) -> AthenaTableMetadata:
        """Build the metadata Athena reports for a Glue table.

        Athena flattens the storage descriptor into the table parameters: the
        location and formats are always present, the SerDe library whenever
        the descriptor has SerDe information, and SerDe parameters with a
        ``serde.param.`` prefix. The Glue description is not the table comment.
        Glue keeps an Iceberg table's dropped and renamed columns, marked as
        not current, which Athena leaves out.

        Args:
            table: A ``Table`` from a Glue ``GetTable`` or ``GetTables`` response.

        Returns:
            The table's metadata as Athena reports it.
        """
        descriptor = table.get("StorageDescriptor") or {}
        parameters = dict(table.get("Parameters") or {})
        parameters["location"] = descriptor.get("Location")
        parameters["inputformat"] = descriptor.get("InputFormat")
        parameters["outputformat"] = descriptor.get("OutputFormat")
        if "SerdeInfo" in descriptor:
            serde = descriptor["SerdeInfo"]
            parameters["serde.serialization.lib"] = serde.get("SerializationLibrary")
            parameters.update(
                {f"serde.param.{k}": v for k, v in (serde.get("Parameters") or {}).items()}
            )

        def column(c: Mapping[str, Any]) -> dict[str, Any]:
            return {k: c[k] for k in ("Name", "Type", "Comment") if k in c}

        return AthenaTableMetadata(
            {
                "TableMetadata": {
                    "Name": table.get("Name"),
                    "CreateTime": table.get("CreateTime"),
                    "LastAccessTime": table.get("LastAccessTime"),
                    "TableType": table.get("TableType"),
                    "Columns": [
                        column(c)
                        for c in descriptor.get("Columns") or []
                        if (c.get("Parameters") or {}).get("iceberg.field.current") != "false"
                    ],
                    "PartitionKeys": [column(c) for c in table.get("PartitionKeys") or []],
                    "Parameters": parameters,
                }
            }
        )
