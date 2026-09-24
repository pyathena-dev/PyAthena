# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
"""Read Athena table and database metadata from the AWS Glue Data Catalog."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from botocore.exceptions import (
    ConnectionError,
    HTTPClientError,
    NoCredentialsError,
    NoRegionError,
)

from pyathena.model import AthenaDatabase, AthenaTableMetadata

if TYPE_CHECKING:
    from boto3.session import Session
    from botocore.client import BaseClient
    from botocore.config import Config


class GlueMetadataClient:
    """Reads Athena metadata of Glue-backed catalogs through the Glue API.

    Reports the metadata Athena's ``GetTableMetadata``, ``ListTableMetadata``
    and ``ListDatabases`` would give for the same catalog, from ``GetTable``,
    ``GetTables`` and ``GetDatabases``. A connection holds one, which builds
    its Glue client on first use.

    Args:
        session: The connection's boto3 session.
        region_name: The connection's region.
        config: The connection's botocore config.
        client_kwargs: The connection's client arguments. Athena's
            ``endpoint_url`` and ``api_version`` are not passed to Glue.
    """

    def __init__(
        self,
        session: Session,
        region_name: str | None,
        config: Config | None,
        client_kwargs: Mapping[str, Any],
    ) -> None:
        self._session = session
        self._region_name = region_name
        self._config = config
        self._client_kwargs = {
            k: v for k, v in client_kwargs.items() if k not in ("endpoint_url", "api_version")
        }
        # A boto3 session is not thread-safe, so the client is built once.
        self._lock = threading.Lock()
        self._client: BaseClient | None = None
        self._reachable = True

    # Failures that every later request would repeat: no route to Glue, a
    # timeout, or no credentials or region for it.
    _UNREACHABLE_ERRORS = (ConnectionError, HTTPClientError, NoCredentialsError, NoRegionError)

    @property
    def reachable(self) -> bool:
        """False once a request could not reach Glue."""
        return self._reachable

    @property
    def client(self) -> BaseClient:
        """The Glue client, built on first use."""
        with self._lock:
            if self._client is None:
                self._client = self._session.client(
                    "glue",
                    region_name=self._region_name,
                    config=self._config,
                    **self._client_kwargs,
                )
            return self._client

    @staticmethod
    def _catalog_request_kwargs(catalog_name: str | None) -> dict[str, str] | None:
        # AwsDataCatalog is the caller's default Glue catalog. An S3 Tables
        # catalog is a Glue federated catalog addressed by its Athena name.
        if not catalog_name:
            return None
        lowered = catalog_name.lower()
        if lowered == "awsdatacatalog":
            return {}
        if lowered.startswith("s3tablescatalog/"):
            return {"CatalogId": catalog_name}
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

    def usable_for(self, catalog_name: str | None) -> bool:
        """Whether to ask Glue about the catalog.

        False once a request could not reach Glue, so later requests do not
        wait for it again.
        """
        return self._reachable and self.supports(catalog_name)

    def _request(self, operation: str, catalog_name: str | None, **kwargs: Any) -> Any:
        request_kwargs = self._catalog_request_kwargs(catalog_name)
        if request_kwargs is None:
            raise ValueError(f"Glue cannot answer for the catalog {catalog_name!r}.")
        try:
            if operation == "get_table":
                return self.client.get_table(**kwargs, **request_kwargs)
            pages = self.client.get_paginator(operation).paginate(**kwargs, **request_kwargs)
            return list(pages)
        except self._UNREACHABLE_ERRORS:
            self._reachable = False
            raise

    def get_table(
        self, catalog_name: str | None, schema_name: str | None, table_name: str
    ) -> AthenaTableMetadata:
        """Get one table's metadata with ``GetTable``."""
        response = self._request(
            "get_table", catalog_name, DatabaseName=schema_name, Name=table_name
        )
        return self.table_metadata(response["Table"])

    def list_tables(
        self, catalog_name: str | None, schema_name: str | None, expression: str | None = None
    ) -> list[AthenaTableMetadata]:
        """List a database's table metadata with ``GetTables``."""
        kwargs: dict[str, Any] = {"DatabaseName": schema_name}
        if expression:
            kwargs["Expression"] = expression
        pages = self._request("get_tables", catalog_name, **kwargs)
        return [self.table_metadata(t) for page in pages for t in page["TableList"]]

    def list_databases(self, catalog_name: str | None) -> list[AthenaDatabase]:
        """List the catalog's databases with ``GetDatabases``."""
        pages = self._request("get_databases", catalog_name)
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
