# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import json
from dataclasses import replace

import pytest
from pyathena_bench.__main__ import main
from pyathena_bench.cases import SQLALCHEMY_FAMILIES, matrix
from pyathena_bench.config import Settings, identifier, select_sql


@pytest.mark.parametrize("value", ["x; DROP TABLE data", "a.b", "a'", "../source", "A", ""])
def test_identifier_rejects_sql_and_path_syntax(value):
    with pytest.raises(ValueError, match="Invalid identifier"):
        identifier(value)


@pytest.mark.parametrize(
    "changes",
    [
        {"arraysizes": [1001]},
        {"chunksizes": [0]},
        {"warmups": -1},
        {"repetitions": 0},
        {"scales": {"small": True}},
        {"poll_interval": float("nan")},
        {"memory_fraction": 1},
        {"filesystem_sizes": {"small": 0}},
        {"filesystem_block_size": 2**20},
        {"filesystem_sizes": {"small": 10_001 * 8 * 2**20}},
        {"filesystem_sizes": {"small": 8 * 2**20}},
        {"filesystem_max_workers": []},
        {"filesystem_pool_connections": [0]},
        {"objects_counts": {}},
        {"objects_counts": {"small": 0}},
        {"objects_size": 0},
        {"objects_listing_keys_per_object": 0},
        {"objects_size": 5 * 2**30 + 1},
        {"objects_concurrency": []},
    ],
)
def test_invalid_measurement_settings(changes):
    with pytest.raises(ValueError, match=r"Invalid|positive|must"):
        replace(Settings(), **changes)


def test_matrix_preserves_unsupported_capabilities():
    cases = matrix(Settings(), "single", "nested")
    assert len({c.id for c in cases}) == len(cases)
    wrangler = [c for c in cases if c.family == "wrangler"]
    assert all(c.unsupported for c in wrangler if c.transport == "csv")
    assert all(c.unsupported is None for c in wrangler if c.transport != "csv")
    pandas_unload = [c for c in cases if c.family == "pandas" and c.transport == "unload"]
    # PandasCursor reads UNLOAD results in chunks, with or without dtype_backend.
    assert all(c.unsupported is None for c in pandas_unload)
    assert {(c.chunksize, c.dtype_backend) for c in pandas_unload if c.output == "native"} == {
        (chunk, backend) for chunk in (None, 10000, 100000) for backend in (None, "pyarrow")
    }
    assert {c.api for c in cases if c.family == "cursor"} == {"sync", "thread", "aio"}


def test_sqlalchemy_cases_are_synchronous_single_query_reads():
    cases = [c for c in matrix(Settings(), "single", "flat") if c.family in SQLALCHEMY_FAMILIES]
    assert {(c.family, c.transport) for c in cases} == {
        ("sqla_rest", "csv"),
        ("sqla_s3fs", "csv"),
        *((f"sqla_{d}", t) for d in ("pandas", "arrow", "polars") for t in ("csv", "unload")),
    }
    assert {c.chunksize for c in cases} == {None, 10000, 100000}
    assert {(c.api, c.output, c.unsupported) for c in cases} == {("sync", "native", None)}
    for suite in ("concurrent", "init"):
        assert not [c for c in matrix(Settings(), suite, "flat") if c.family in SQLALCHEMY_FAMILIES]
        # The pyarrow dtype variants belong to the single-query suite too.
        assert not [c for c in matrix(Settings(), suite, "flat") if c.dtype_backend]


def test_json_shape_covers_pandas_and_wrangler_reads():
    cases = matrix(Settings(), "single", "json")
    assert {(c.family, c.transport, c.output) for c in cases} == {
        ("pandas", "csv", "native"),
        ("pandas", "unload", "native"),
        ("wrangler", "csv", "native"),
        ("wrangler", "ctas", "native"),
        ("wrangler", "unload", "native"),
    }
    # Parquet cannot hold json, so UNLOAD and CTAS stay visible as unsupported.
    assert all((c.unsupported is None) == (c.transport == "csv") for c in cases)
    assert {c.chunksize for c in cases if c.transport == "csv"} == {None, 10000, 100000}
    # The initialization suite would also prepare an UNLOAD fixture, which json rejects.
    assert matrix(Settings(), "init", "json") == []
    assert matrix(Settings(), "concurrent", "json") == []


def test_temporal_shape_covers_every_flat_case_in_the_single_query_suite():
    # UNLOAD and CTAS read a Parquet projection instead of being unsupported.
    assert matrix(Settings(), "single", "temporal") == [
        replace(c, shape="temporal") for c in matrix(Settings(), "single", "flat")
    ]
    assert matrix(Settings(), "init", "temporal") == []
    assert matrix(Settings(), "concurrent", "temporal") == []


def test_concurrency_and_init_matrices_are_bounded():
    cases = matrix(Settings(), "concurrent", "flat")
    assert {c.concurrency for c in cases} == {1, 10, 50, 100}
    assert {c.api for c in cases} == {"thread", "aio"}
    initializers = matrix(Settings(), "init", "flat")
    assert {c.api for c in initializers} == {"direct", "to_thread"}
    assert len({c.id for c in initializers}) == len(initializers)


def test_filesystem_matrix_sweeps_workers_and_pools_for_each_api_and_operation():
    settings = replace(Settings(), filesystem_max_workers=[8, 40], filesystem_pool_connections=[10])
    cases = matrix(settings, "filesystem", "flat")
    assert len(cases) == 5 * 3 * 2
    assert {(c.transport, c.api) for c in cases} == {
        (operation, api)
        for operation in ("read", "get", "write", "put", "copy")
        for api in ("sync", "aio", "s3fs")
    }
    assert {(c.max_workers, c.pool_connections) for c in cases} == {(8, 10), (40, 10)}
    assert {"filesystem-s3-aio-copy-w40-p10", "filesystem-s3-s3fs-get-w8-p10"} <= {
        c.id for c in cases
    }
    # The suite does not query Athena, so the other shapes add no cases.
    assert matrix(settings, "filesystem", "nested") == []


def test_filesystem_section_is_loaded_with_its_own_scales(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text(
        "[scales]\nsmall = 10000\n"
        "[filesystem]\nsizes = { large = 1073741824 }\nblock_size = 16777216\n"
        "max_workers = [4]\npool_connections = [4, 16]\n"
    )
    settings = Settings.load(config)
    assert settings.suite_scales("filesystem") == {"large": 2**30}
    assert settings.suite_scales("single") == {"small": 10000}
    assert settings.filesystem_block_size == 16 * 2**20
    assert settings.filesystem_pool_connections == [4, 16]


def test_objects_section_is_loaded_with_its_own_scales(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text(
        "[scales]\nsmall = 10000\n"
        "[objects]\ncounts = { medium = 1000 }\nsize = 4096\nlisting_keys_per_object = 10\n"
        "concurrency = [4]\n"
    )
    settings = Settings.load(config)
    assert settings.suite_scales("objects") == {"medium": 1000}
    assert settings.suite_scales("single") == {"small": 10000}
    assert (settings.objects_size, settings.objects_listing_keys_per_object) == (4096, 10)
    assert settings.objects_concurrency == [4]


def test_objects_matrix_sweeps_the_concurrency_for_each_api_and_operation():
    settings = replace(Settings(), objects_concurrency=[8, 40])
    cases = matrix(settings, "objects", "flat")
    assert len(cases) == 8 * 3 * 2
    assert {(c.transport, c.api) for c in cases} == {
        (operation, api)
        for operation in ("cat", "get", "put", "pipe", "copy", "rm", "find", "info")
        for api in ("sync", "aio", "s3fs")
    }
    # The concurrency is the filesystem's workers and pool, and the batch size.
    assert {(c.max_workers, c.pool_connections) for c in cases} == {(8, 8), (40, 40)}
    assert all(
        c.output == "objects" and c.unsupported_reason(settings, "small") is None for c in cases
    )
    assert "objects-s3-s3fs-cat-n40" in {c.id for c in cases}
    assert matrix(settings, "objects", "nested") == []


def test_flat_projection_and_nested_projection_share_the_snapshot():
    flat = select_sql("scratch", "b_run_small", "flat", "csv")
    nested = select_sql("scratch", "b_run_small", "nested", "csv")
    assert flat.endswith('FROM "scratch"."b_run_small"')
    assert nested.endswith('FROM "scratch"."b_run_small"')
    assert "file.filename AS filename" in flat
    assert "project, file, details, http" in nested
    assert "LIMIT" not in flat
    json = select_sql("scratch", "b_run_small", "json", "csv")
    assert json == flat.replace(
        ' FROM "scratch"', ', CAST(details AS JSON) AS details_json FROM "scratch"'
    )
    # The other shapes read the same projection with every transport.
    for shape in ("flat", "nested", "json"):
        for transport in ("unload", "ctas"):
            assert select_sql("scratch", "b_run_small", shape, transport) == select_sql(
                "scratch", "b_run_small", shape, "csv"
            )


def test_temporal_projection_of_parquet_reads_has_no_time_or_time_zone_types():
    flat = select_sql("scratch", "b_run_small", "flat", "csv")
    parquet = flat.replace(
        ' FROM "scratch"',
        ', from_unixtime("timestamp" / 1e6) AS event_time, '
        'CAST(from_unixtime("timestamp" / 1e6) AS date) AS event_date FROM "scratch"',
    )
    # Athena's UNLOAD and CTAS fail with "Unsupported Hive type" for time and with
    # time zone columns.
    assert select_sql("scratch", "b_run_small", "temporal", "unload") == parquet
    assert select_sql("scratch", "b_run_small", "temporal", "ctas") == parquet
    assert select_sql("scratch", "b_run_small", "temporal", "csv") == parquet.replace(
        ' FROM "scratch"',
        ", from_unixtime(\"timestamp\" / 1e6, 'UTC') AS event_time_utc, "
        "from_unixtime(\"timestamp\" / 1e6, 'Asia/Tokyo') AS event_time_zone, "
        "from_unixtime(\"timestamp\" / 1e6, '+09:00') AS event_time_offset, "
        'CAST(from_unixtime("timestamp" / 1e6) AS time(3)) AS event_clock FROM "scratch"',
    )
    # A misspelled transport fails instead of reading the projection of CSV results.
    with pytest.raises(ValueError, match="Unknown transport"):
        select_sql("scratch", "b_run_small", "temporal", "UNLOAD")


@pytest.mark.parametrize("family", ["cursor", "dict", "arrow"])
def test_plan_page_warning_uses_smallest_selected_arraysize(tmp_path, capsys, family):
    config = tmp_path / "config.toml"
    config.write_text("[scales]\nxlarge = 10000000\n")
    assert (
        main(
            [
                "--config",
                str(config),
                "plan",
                "--suite",
                "single",
                "--scale",
                "xlarge",
                "--family",
                family,
            ]
        )
        == 0
    )
    warnings = json.loads(capsys.readouterr().out)["warnings"]
    if family == "arrow":
        assert warnings == []
    else:
        assert "arraysize 100 needs >= 100000" in warnings[0]
