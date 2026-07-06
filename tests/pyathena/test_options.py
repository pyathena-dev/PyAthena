import dataclasses

import pytest

from pyathena.options import ExecuteOptions


def test_execute_options_defaults():
    options = ExecuteOptions()
    assert options.work_group is None
    assert options.s3_staging_dir is None
    assert options.cache_size == 0
    assert options.cache_expiration_time == 0
    assert options.result_reuse_enable is None
    assert options.result_reuse_minutes is None
    assert options.paramstyle is None
    assert options.on_start_query_execution is None
    assert options.result_set_type_hints is None


def test_execute_options_is_frozen():
    options = ExecuteOptions()
    with pytest.raises(dataclasses.FrozenInstanceError):
        options.work_group = "primary"  # type: ignore[misc]


def test_merge_ignores_none_overrides():
    options = ExecuteOptions(work_group="primary", cache_size=100)
    merged = options.merge(work_group=None, cache_size=None, s3_staging_dir=None)
    assert merged.work_group == "primary"
    assert merged.cache_size == 100
    assert merged.s3_staging_dir is None


def test_merge_overrides_take_precedence():
    options = ExecuteOptions(
        work_group="primary",
        cache_size=100,
        result_reuse_enable=True,
        paramstyle="pyformat",
    )
    merged = options.merge(
        work_group="adhoc",
        cache_size=0,
        result_reuse_enable=False,
        paramstyle="qmark",
    )
    assert merged.work_group == "adhoc"
    assert merged.cache_size == 0
    assert merged.result_reuse_enable is False
    assert merged.paramstyle == "qmark"


def test_merge_does_not_mutate_original():
    options = ExecuteOptions(work_group="primary")
    merged = options.merge(work_group="adhoc")
    assert options.work_group == "primary"
    assert merged is not options


def test_merge_without_applied_overrides_returns_self():
    options = ExecuteOptions(work_group="primary")
    assert options.merge() is options
    assert options.merge(work_group=None) is options


def test_merge_raises_on_unknown_field():
    with pytest.raises(TypeError, match="unknown_field"):
        ExecuteOptions().merge(unknown_field="value")


@pytest.mark.parametrize(
    ("base", "overrides", "expected"),
    [
        # Overrides fill in unset fields
        (
            {},
            {"work_group": "wg", "result_reuse_minutes": 5},
            {"work_group": "wg", "result_reuse_minutes": 5},
        ),
        # False is a real value and must override
        (
            {"result_reuse_enable": True},
            {"result_reuse_enable": False},
            {"result_reuse_enable": False},
        ),
        # 0 is a real value and must override
        ({"cache_size": 100}, {"cache_size": 0}, {"cache_size": 0}),
        # Callbacks and hints pass through
        (
            {"result_set_type_hints": {"tags": "array(varchar)"}},
            {"result_set_type_hints": {"tags": "map(varchar, integer)"}},
            {"result_set_type_hints": {"tags": "map(varchar, integer)"}},
        ),
    ],
)
def test_merge_precedence(base, overrides, expected):
    merged = ExecuteOptions(**base).merge(**overrides)
    for name, value in expected.items():
        assert getattr(merged, name) == value
