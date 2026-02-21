# -*- coding: utf-8 -*-
import contextlib
from urllib.parse import quote_plus

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
from sqlalchemy.testing.plugin.pytestplugin import (
    pytest_sessionstart as sqlalchemy_pytest_sessionstart,
)
from sqlalchemy.testing.provision import (
    configure_follower,
    create_db,
    drop_db,
    temp_table_keyword_args,
)

from tests import ASYNC_SQLALCHEMY_CONNECTION_STRING, ENV, SQLALCHEMY_CONNECTION_STRING


def pytest_sessionstart(session):
    use_async = session.config.getoption("--dburi", None) == ["async"]
    base_conn_str = (
        ASYNC_SQLALCHEMY_CONNECTION_STRING if use_async else SQLALCHEMY_CONNECTION_STRING
    )
    conn_str = base_conn_str + "&tblproperties=" + quote_plus("'table_type'='ICEBERG'")
    session.config.option.dburi = [
        conn_str.format(
            region_name=ENV.region_name,
            schema_name=ENV.schema,
            s3_staging_dir=ENV.s3_staging_dir,
            location=ENV.s3_staging_dir,
        )
    ]
    sqlalchemy_pytest_sessionstart(session)


@create_db.for_db("awsathena")
def _awsathena_create_db(cfg, eng, ident):
    with eng.begin() as conn, contextlib.suppress(Exception):
        _awsathena_drop_db(cfg, conn, ident)

    with eng.begin() as conn:
        conn.exec_driver_sql(f"CREATE DATABASE {ident}")
        conn.exec_driver_sql(f"CREATE DATABASE {ident}_test_schema")
        conn.exec_driver_sql(f"CREATE DATABASE {ident}_test_schema_2")


@drop_db.for_db("awsathena")
def _awsathena_drop_db(cfg, eng, ident):
    with eng.begin() as conn:
        conn.exec_driver_sql(f"DROP DATABASE {ident} CASCADE")
        conn.exec_driver_sql(f"DROP DATABASE {ident}_test_schema CASCADE")
        conn.exec_driver_sql(f"DROP DATABASE {ident}_test_schema_2 CASCADE")


@configure_follower.for_db("awsathena")
def _awsathena_configure_follower(config, ident):
    config.test_schema = f"{ident}_test_schema"
    config.test_schema_2 = f"{ident}_test_schema_2"


@temp_table_keyword_args.for_db("awsathena")
def _awsathena_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
