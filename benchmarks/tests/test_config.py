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
from pyathena_bench.cases import matrix
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
    assert all(
        c.unsupported
        for c in cases
        if c.family == "pandas" and c.transport == "unload" and c.chunksize
    )
    assert {c.api for c in cases if c.family == "cursor"} == {"sync", "thread", "aio"}


def test_concurrency_and_init_matrices_are_bounded():
    cases = matrix(Settings(), "concurrent", "flat")
    assert {c.concurrency for c in cases} == {1, 10, 50, 100}
    assert {c.api for c in cases} == {"thread", "aio"}
    initializers = matrix(Settings(), "init", "flat")
    assert {c.api for c in initializers} == {"direct", "to_thread"}
    assert len({c.id for c in initializers}) == len(initializers)


def test_flat_projection_and_nested_projection_share_the_snapshot():
    flat = select_sql("scratch", "b_run_small", "flat")
    nested = select_sql("scratch", "b_run_small", "nested")
    assert flat.endswith('FROM "scratch"."b_run_small"')
    assert nested.endswith('FROM "scratch"."b_run_small"')
    assert "file.filename AS filename" in flat
    assert "project, file, details, http" in nested
    assert "LIMIT" not in flat


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
