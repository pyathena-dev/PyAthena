# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import dataclasses

import pytest

from pyathena.options import ExecuteOptions


class TestExecuteOptions:
    def test_defaults(self):
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

    def test_is_frozen(self):
        options = ExecuteOptions()
        with pytest.raises(dataclasses.FrozenInstanceError):
            options.work_group = "primary"  # type: ignore[misc]

    def test_merge_ignores_none_overrides(self):
        options = ExecuteOptions(work_group="primary", cache_size=100)
        merged = options.merge(work_group=None, cache_size=None, s3_staging_dir=None)
        assert merged.work_group == "primary"
        assert merged.cache_size == 100
        assert merged.s3_staging_dir is None

    def test_merge_overrides_take_precedence(self):
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

    def test_merge_does_not_mutate_original(self):
        options = ExecuteOptions(work_group="primary")
        merged = options.merge(work_group="adhoc")
        assert options.work_group == "primary"
        assert merged is not options

    def test_merge_without_applied_overrides_is_equal(self):
        options = ExecuteOptions(work_group="primary")
        assert options.merge() == options
        assert options.merge(work_group=None) == options

    def test_merge_raises_on_unknown_field(self):
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
    def test_merge_precedence(self, base, overrides, expected):
        merged = ExecuteOptions(**base).merge(**overrides)
        for name, value in expected.items():
            assert getattr(merged, name) == value

    def test_resolve_with_none_returns_defaults_with_overrides(self):
        resolved = ExecuteOptions.resolve(None, work_group="wg")
        assert resolved == ExecuteOptions(work_group="wg")

    def test_resolve_applies_overrides_to_given_options(self):
        options = ExecuteOptions(work_group="primary", cache_size=100)
        resolved = ExecuteOptions.resolve(options, work_group="adhoc", cache_size=None)
        assert resolved == ExecuteOptions(work_group="adhoc", cache_size=100)

    @pytest.mark.parametrize(
        ("overrides", "expected"),
        [
            ({"cache_size": None}, {"cache_size": 0}),
            ({"cache_size": 0}, {"cache_size": 0}),
            ({"result_reuse_enable": False}, {"result_reuse_enable": False}),
            ({"work_group": ""}, {"work_group": ""}),
            ({"work_group": "primary"}, {"work_group": "primary"}),
            ({"unknown_field": None}, {}),
        ],
    )
    def test_resolve_without_base_preserves_override_values(self, overrides, expected):
        resolved = ExecuteOptions.resolve(None, **overrides)
        for name, value in expected.items():
            assert getattr(resolved, name) == value
        assert resolved.cache_expiration_time == 0
        assert resolved.result_reuse_minutes is None

    def test_resolve_returns_distinct_default_instances(self):
        first = ExecuteOptions.resolve(None)
        second = ExecuteOptions.resolve(None)
        assert first == second
        assert first is not second

    def test_resolve_preserves_callback_and_hint_references(self):
        def callback(query_id):
            pass

        first_hints = {"tags": "array(varchar)"}
        second_hints = {"tags": "map(varchar, integer)"}
        first = ExecuteOptions.resolve(
            None, on_start_query_execution=callback, result_set_type_hints=first_hints
        )
        second = ExecuteOptions.resolve(None, result_set_type_hints=second_hints)
        assert first.on_start_query_execution is callback
        assert first.result_set_type_hints is first_hints
        assert second.result_set_type_hints is second_hints
        first_hints["tags"] = "array(bigint)"
        assert second.result_set_type_hints == {"tags": "map(varchar, integer)"}

    @pytest.mark.parametrize("options", [None, ExecuteOptions(work_group="primary")])
    def test_resolve_rejects_unknown_non_none_fields(self, options):
        with pytest.raises(TypeError, match="unknown_field"):
            ExecuteOptions.resolve(options, unknown_field="value")

    def test_resolve_given_options_preserves_new_instance_behavior(self):
        options = ExecuteOptions(work_group="primary")
        resolved = ExecuteOptions.resolve(options, work_group=None)
        assert resolved == options
        assert resolved is not options

    @pytest.mark.parametrize("provided", [False, True])
    def test_resolve_retains_subclass_merge_dispatch(self, provided):
        @dataclasses.dataclass(frozen=True)
        class CustomOptions(ExecuteOptions):
            route: str = "custom"

            def merge(self, **overrides):
                overrides["work_group"] = self.route
                return super().merge(**overrides)

        if provided:
            base = CustomOptions(route="existing")
            resolved = ExecuteOptions.resolve(base, cache_size=10)
            expected = "existing"
        else:
            resolved = CustomOptions.resolve(None, cache_size=10)
            expected = "custom"
        assert isinstance(resolved, CustomOptions)
        assert resolved.work_group == expected
        assert resolved.cache_size == 10
