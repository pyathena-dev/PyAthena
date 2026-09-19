import warnings

import pytest
from sqlalchemy import Integer, String, all_, any_, bindparam, column, func, select, table, types
from sqlalchemy import exc as sa_exc
from sqlalchemy.sql import operators
from sqlalchemy.sql.compiler import FROM_LINTING

from pyathena.sqlalchemy.base import AthenaDialect
from pyathena.sqlalchemy.types import AthenaArray


def compile_sql(expression):
    return str(expression.compile(dialect=AthenaDialect(), compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize("array_type", [AthenaArray(Integer), types.ARRAY(Integer)])
@pytest.mark.parametrize("index", [-2, 0, 1, 100])
def test_array_index(array_type, index):
    value = column("items", array_type)
    assert compile_sql(value[index]) == f"element_at(items, IF({index} > 0, {index}, NULL))"
    assert isinstance(value[index].type, Integer)


def test_array_dimensions_and_zero_indexes():
    value = column("items", AthenaArray(Integer, dimensions=3, zero_indexes=True, as_tuple=True))
    assert value[0].type.dimensions == 2
    assert value[0][0].type.dimensions == 1
    assert isinstance(value[0][0][0].type, Integer)
    assert value[0].type.as_tuple
    assert value[0].type.zero_indexes
    assert "IF(1 > 0, 1, NULL)" in compile_sql(value[0])
    assert "greatest(1, 1)" in compile_sql(value[:0])
    assert "least(1, cardinality(items))" in compile_sql(value[:0])
    assert "cardinality(items)" in compile_sql(value[0:])
    nested = column("nested", AthenaArray(AthenaArray(String)))
    assert isinstance(nested[1].type, AthenaArray)
    assert isinstance(nested[1][1].type, String)


@pytest.mark.parametrize(
    "bounds", [slice(None), slice(1, 2), slice(-2, 100), slice(3, 1), slice(1, 3, 1)]
)
def test_array_slice(bounds):
    value = column("items", AthenaArray(Integer))
    result = value[bounds]
    assert result.type is value.type
    sql = compile_sql(result)
    assert sql.startswith("slice(items, greatest(")
    assert "greatest(least(" in sql


@pytest.mark.parametrize("step", [0, 2, -1, True, 1.0, bindparam("step", 1)])
def test_array_slice_rejects_steps(step):
    with pytest.raises(sa_exc.CompileError, match="step"):
        compile_sql(column("items", AthenaArray(Integer))[1:3:step])


@pytest.mark.parametrize(("aggregate", "function"), [(any_, "any_match"), (all_, "all_match")])
@pytest.mark.parametrize(
    "op", [operators.eq, operators.ne, operators.lt, operators.le, operators.gt, operators.ge]
)
def test_array_quantified_comparison(aggregate, function, op):
    items = column("items", AthenaArray(Integer))
    sql = compile_sql(op(2, aggregate(items)))
    assert sql.startswith(f"{function}((items), _pyathena_element_0 -> 2 ")
    assert "_pyathena_element_0)" in sql


def test_array_quantifier_null_negation_and_legacy_methods():
    items = column("items", AthenaArray(Integer))
    assert "NULL = _pyathena_element_0" in compile_sql(any_(items) == None)  # noqa: E711
    assert compile_sql(items.any(2)) == compile_sql(any_(items) == 2)
    assert compile_sql(items.all(2, operator=operators.lt)) == compile_sql(all_(items) > 2)
    assert compile_sql(~items.any(2)).startswith("NOT (any_match(")
    assert "2 > _pyathena_element_0" in compile_sql(any_(items) < 2)


def test_array_lambda_does_not_capture_column_names():
    items = column("items", AthenaArray(Integer))
    scalar = column("_pyathena_element_0", Integer)
    sql = compile_sql(scalar == any_(items))
    assert "_pyathena_element_1 -> _pyathena_element_0 = _pyathena_element_1" in sql


def test_multidimensional_array_quantifier_bind_type():
    items = column("items", AthenaArray(Integer, dimensions=2))
    assert "CAST(ARRAY[1, 2] AS ARRAY(INTEGER)) =" in compile_sql(items.any([1, 2]))


def test_subquery_any_remains_unchanged():
    sql = compile_sql(any_(select(column("item", Integer)).scalar_subquery()) == 2)
    assert "ANY (SELECT item)" in sql
    assert "any_match" not in sql
    items = column("items", AthenaArray(Integer))
    array_subquery = compile_sql(select(items == any_(select(items).scalar_subquery())))
    assert "ANY (SELECT items" in array_subquery
    assert "any_match" not in array_subquery


def test_array_concat_and_cache_bind_values():
    items = column("items", AthenaArray(Integer))
    assert compile_sql(items.concat([2])) == "items || CAST(ARRAY[2] AS ARRAY(INTEGER))"
    first = select(items[bindparam("index")]).where(any_(items) == 2)
    second = select(items[bindparam("index")]).where(any_(items) == 3)
    assert first._generate_cache_key().key == second._generate_cache_key().key
    assert compile_sql(first.params(index=1)) != compile_sql(first.params(index=2))


def test_quantifier_boolean_left_operand():
    flags = column("flags", AthenaArray(types.Boolean))
    assert "any_match" in compile_sql(any_(flags) == True)  # noqa: E712
    assert "all_match" in compile_sql(all_(flags) != False)  # noqa: E712
    assert "IS DISTINCT FROM" in compile_sql(any_(flags).is_distinct_from(True))


def test_quantifier_join_linter_tracks_original_tables():
    left = table("left_table", column("value", Integer))
    right = table("right_table", column("items", AthenaArray(Integer)))
    query = select(left, right).where(left.c.value == any_(right.c["items"]))
    with warnings.catch_warnings():
        warnings.simplefilter("error", sa_exc.SAWarning)
        query.compile(dialect=AthenaDialect(), linting=FROM_LINTING)


def test_generic_array_slice_step_is_rendered_for_cache_validation():
    items = column("items", types.ARRAY(Integer))
    query = select(items[2:3:1])
    compiled = query.compile(dialect=AthenaDialect())
    assert "Unsupported ARRAY slice step" in str(compiled)
    assert list(compiled.params.values()).count(1) == 1
    assert query._generate_cache_key().key == select(items[2:3:2])._generate_cache_key().key
    step_name = next(name for name, value in compiled.params.items() if value == 1)
    assert f"IF(%({step_name})s = 1" in str(compiled)
    for invalid in (2, 1.0, None):
        with pytest.raises(ValueError, match="step"):
            compiled._bind_processors[step_name](invalid)
    native = column("items", AthenaArray(Integer))
    assert (
        select(native[1:3:1])._generate_cache_key().key
        == select(native[1:3])._generate_cache_key().key
    )
    with pytest.raises(sa_exc.CompileError, match="step"):
        native[1:3:2]


def test_stepped_array_aggregate_with_inferred_element_type():
    expression = func.array_agg(func.length("abc"))[1:2:1]
    sql = compile_sql(select(expression))
    assert "array_agg(length('abc'))" in sql
    assert "Unsupported ARRAY slice step" in sql
    assert "ARRAY(NULL)" not in sql
