from __future__ import annotations

import re
from collections.abc import Mapping
from itertools import product
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import exc, select, types, util
from sqlalchemy.sql import operators, visitors
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)
from sqlalchemy.sql.elements import (
    BindParameter,
    Cast,
    CollectionAggregate,
    Null,
    Slice,
    TextClause,
    UnaryExpression,
    _label_reference,
    _textual_label_reference,
)
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.selectable import CompoundSelect, ScalarSelect

from pyathena.model import (
    AthenaFileFormat,
    AthenaPartitionTransform,
    AthenaRowFormatSerde,
)
from pyathena.sqlalchemy.array import _ArraySliceStepType, _ArrayTypeInspector
from pyathena.sqlalchemy.preparer import AthenaDDLIdentifierPreparer
from pyathena.sqlalchemy.types import (
    AthenaMap,
    AthenaStruct,
    get_double_type,
)
from pyathena.sqlalchemy.util import _split_type_arguments

if TYPE_CHECKING:
    from sqlalchemy import (
        CheckConstraint,
        ForeignKeyConstraint,
        PrimaryKeyConstraint,
        Table,
        UniqueConstraint,
    )
    from sqlalchemy.sql.ddl import CreateTable
    from sqlalchemy.sql.functions import Function
    from sqlalchemy.sql.selectable import GenerativeSelect

    from pyathena.sqlalchemy.base import AthenaDialect

    _DialectArgDict = Mapping[str, Any]
    CreateColumn = Any

# Prefix of the Athena data catalog name registered for an Amazon S3 Tables
# table bucket (e.g. ``s3tablescatalog/my-bucket``). It is selected via the
# connection ``catalog_name``. S3 Tables are Iceberg-backed and use managed
# storage, so their CREATE TABLE statements must not include a LOCATION clause.
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrations-query-athena.html
S3_TABLES_CATALOG_PREFIX = "s3tablescatalog/"


class AthenaTypeCompiler(GenericTypeCompiler):
    """Type compiler for Amazon Athena SQL types.

    This compiler translates SQLAlchemy type objects into Athena-compatible
    SQL type strings for use in DDL statements. It handles the mapping between
    SQLAlchemy's portable types and Athena's specific type syntax.

    Athena has specific requirements for type names that differ from standard
    SQL. For example, FLOAT maps to REAL in CAST expressions, and various
    string types (TEXT, NCHAR, NVARCHAR) all map to STRING.

    The compiler also supports Athena-specific complex types:
    - STRUCT/ROW: Nested record types with named fields
    - MAP: Key-value pair collections
    - ARRAY: Ordered collections of elements

    See Also:
        AWS Athena Data Types:
        https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    """

    def visit_FLOAT(self, type_: types.Float[Any], **kw: Any) -> str:
        return self.visit_REAL(type_, **kw)  # type: ignore[arg-type]

    def visit_REAL(self, type_: types.REAL[Any], **kw: Any) -> str:
        return "FLOAT"

    def visit_DOUBLE(self, type_, **kw) -> str:
        return "DOUBLE"

    def visit_DOUBLE_PRECISION(self, type_, **kw) -> str:
        return "DOUBLE"

    def visit_NUMERIC(self, type_: types.Numeric[Any], **kw: Any) -> str:
        return self.visit_DECIMAL(type_, **kw)  # type: ignore[arg-type]

    def visit_DECIMAL(self, type_: types.DECIMAL[Any], **kw: Any) -> str:
        if type_.precision is None:
            return "DECIMAL"
        if type_.scale is None:
            return f"DECIMAL({type_.precision})"
        return f"DECIMAL({type_.precision}, {type_.scale})"

    def visit_TINYINT(self, type_: types.Integer, **kw: Any) -> str:
        return "TINYINT"

    def visit_INTEGER(self, type_: types.Integer, **kw: Any) -> str:
        return "INT" if kw.get("_athena_array_ddl") else "INTEGER"

    def visit_SMALLINT(self, type_: types.SmallInteger, **kw: Any) -> str:
        return "SMALLINT"

    def visit_BIGINT(self, type_: types.BigInteger, **kw: Any) -> str:
        return "BIGINT"

    def visit_TIMESTAMP(self, type_: types.TIMESTAMP, **kw: Any) -> str:
        return "TIMESTAMP"

    def visit_DATETIME(self, type_: types.DateTime, **kw: Any) -> str:
        return self.visit_TIMESTAMP(type_, **kw)  # type: ignore[arg-type]

    def visit_DATE(self, type_: types.Date, **kw: Any) -> str:
        return "DATE"

    def visit_TIME(self, type_: types.Time, **kw: Any) -> str:
        raise exc.CompileError(f"Data type `{type_}` is not supported")

    def visit_CLOB(self, type_: types.CLOB, **kw: Any) -> str:
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_NCLOB(self, type_: types.Text, **kw: Any) -> str:
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_CHAR(self, type_: types.CHAR, **kw: Any) -> str:
        if type_.length:
            return self._render_string_type("CHAR", type_.length, type_.collation)
        return "STRING"

    def visit_NCHAR(self, type_: types.NCHAR, **kw: Any) -> str:
        return self.visit_CHAR(type_, **kw)  # type: ignore[arg-type]

    def visit_VARCHAR(self, type_: types.String, **kw: Any) -> str:
        if type_.length:
            return self._render_string_type("VARCHAR", type_.length, type_.collation)
        return "STRING"

    def visit_NVARCHAR(self, type_: types.NVARCHAR, **kw: Any) -> str:
        return self.visit_VARCHAR(type_, **kw)  # type: ignore[arg-type]

    def visit_TEXT(self, type_: types.Text, **kw: Any) -> str:
        return "STRING"

    def visit_BLOB(self, type_: types.LargeBinary, **kw: Any) -> str:
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_BINARY(self, type_: types.BINARY, **kw: Any) -> str:
        return "BINARY"

    def visit_VARBINARY(self, type_: types.VARBINARY, **kw: Any) -> str:
        return self.visit_BINARY(type_, **kw)  # type: ignore[arg-type]

    def visit_BOOLEAN(self, type_: types.Boolean, **kw: Any) -> str:
        return "BOOLEAN"

    def visit_JSON(self, type_: types.JSON, **kw: Any) -> str:
        return "JSON"

    def visit_string(self, type_, **kw):
        return "STRING"

    def visit_unicode(self, type_, **kw):
        return "STRING"

    def visit_unicode_text(self, type_, **kw):
        return "STRING"

    def visit_null(self, type_, **kw):
        return "NULL"

    def visit_tinyint(self, type_, **kw):
        return self.visit_TINYINT(type_, **kw)

    def visit_enum(self, type_, **kw):
        return self.visit_string(type_, **kw)

    def visit_struct(self, type_, **kw):
        if isinstance(type_, AthenaStruct):
            if type_.fields:
                field_specs = []
                for field_name, field_type in type_.fields.items():
                    field_type_str = self.process(field_type, **kw)
                    preparer = (
                        AthenaDDLIdentifierPreparer(self.dialect)
                        if kw.get("_athena_array_ddl")
                        else self.dialect.identifier_preparer
                    )
                    name = preparer.quote(field_name)
                    separator = ":" if kw.get("_athena_array_ddl") else " "
                    field_specs.append(f"{name}{separator}{field_type_str}")
                if kw.get("_athena_array_ddl"):
                    return f"STRUCT<{', '.join(field_specs)}>"
                return f"ROW({', '.join(field_specs)})"
            return "ROW()"
        return "ROW()"

    def visit_STRUCT(self, type_, **kw):
        return self.visit_struct(type_, **kw)

    def visit_map(self, type_, **kw):
        if isinstance(type_, AthenaMap):
            key_type_str = self.process(type_.key_type, **kw)
            value_type_str = self.process(type_.value_type, **kw)
            return f"MAP<{key_type_str}, {value_type_str}>"
        return "MAP<STRING, STRING>"

    def visit_MAP(self, type_, **kw):
        return self.visit_map(type_, **kw)

    def visit_array(self, type_, **kw):
        if isinstance(type_, types.ARRAY):
            kw["_athena_array_ddl"] = True
            item_type_str = self.process(_ArrayTypeInspector.item_type(type_), **kw)
            return f"ARRAY<{item_type_str}>"
        return "ARRAY<STRING>"

    def visit_ARRAY(self, type_, **kw):
        return self.visit_array(type_, **kw)


class AthenaStatementCompiler(SQLCompiler):
    """SQL statement compiler for Amazon Athena queries.

    This compiler generates Athena-compatible SQL statements from SQLAlchemy
    expression constructs. It handles Athena-specific SQL syntax including:

    - Function name mapping (e.g., char_length -> length)
    - Lambda expressions in functions like filter()
    - CAST expressions with Athena type requirements
    - OFFSET/LIMIT clause ordering (Athena uses OFFSET before LIMIT)
    - Time travel hints (FOR TIMESTAMP AS OF, FOR VERSION AS OF)

    The compiler ensures that generated SQL is compatible with Presto/Trino
    syntax used by Athena engine versions 2 and 3.

    See Also:
        AWS Athena SQL Reference:
        https://docs.aws.amazon.com/athena/latest/ug/ddl-sql-reference.html
    """

    @util.memoized_property
    def _array_type_inspector(self):
        return _ArrayTypeInspector(self.dialect)

    def visit_char_length_func(self, fn: Function[Any], **kw: Any) -> str:
        return f"length{self.function_argspec(fn, **kw)}"

    @staticmethod
    def _original_froms(elements):
        for element in elements:
            while element._is_clone_of is not None:
                element = element._is_clone_of
            yield element

    def _array_lambda_name(self):
        names = {
            str(
                element.text if isinstance(element, TextClause) else getattr(element, "name", "")
            ).lower()
            for element in visitors.iterate(self.statement)
        }
        index = getattr(self, "_array_lambda_index", 0)
        # Textual SQL can embed a column name inside a larger expression.
        while any(f"_pyathena_element_{index}" in name for name in names):
            index += 1
        self._array_lambda_index = index + 1
        return f"_pyathena_element_{index}"

    def visit_binary(
        self,
        binary,
        override_operator=None,
        eager_grouping=False,
        from_linter=None,
        lateral_from_linter=None,
        **kw,
    ):
        kw.update(
            eager_grouping=eager_grouping,
            from_linter=from_linter,
            lateral_from_linter=lateral_from_linter,
        )
        aggregate = binary.right
        aggregate_on_left = isinstance(binary.left, CollectionAggregate)
        if aggregate_on_left:
            aggregate = binary.left
        array_type = (
            self._array_type_inspector.array_type(aggregate.element.type)
            if isinstance(aggregate, CollectionAggregate)
            and not isinstance(aggregate.element, ScalarSelect)
            else None
        )
        if array_type is not None:
            variable = self._array_lambda_name()
            predicate = binary._clone()
            item_type = _ArrayTypeInspector.item_type(array_type)
            if aggregate_on_left:
                predicate.left = Column(variable, item_type)
            else:
                predicate.right = Column(variable, item_type)
                if isinstance(predicate.left, BindParameter) and (
                    isinstance(item_type, types.ARRAY)
                    or (
                        predicate.left.type is aggregate.element.type
                        and predicate.left.type._type_affinity is not types.ARRAY
                    )
                ):
                    predicate.left = predicate.left._with_binary_element_type(item_type)
            if from_linter is not None and operators.is_comparison(binary.operator):
                if lateral_from_linter is not None:
                    enclosing = [kw["enclosing_lateral"]]
                    lateral_from_linter.edges.update(
                        product(
                            self._original_froms(binary.left._from_objects + enclosing),
                            self._original_froms(binary.right._from_objects + enclosing),
                        )
                    )
                else:
                    from_linter.edges.update(
                        product(
                            self._original_froms(binary.left._from_objects),
                            self._original_froms(binary.right._from_objects),
                        )
                    )
            sql = super().visit_binary(predicate, override_operator=override_operator, **kw)
            function = "any_match" if aggregate.operator is operators.any_op else "all_match"
            array = self.process(aggregate.element, **kw)
            return f"{function}({array}, {variable} -> {sql})"
        return super().visit_binary(binary, override_operator=override_operator, **kw)

    def visit_getitem_binary(self, binary, operator, **kw):
        array_type = self._array_type_inspector.array_type(binary.left.type)
        if array_type is None:
            raise exc.CompileError("Athena indexing requires an ARRAY expression")
        array = self.process(binary.left, **kw)
        if isinstance(binary.right, Slice):
            bounds = binary.right
            if not isinstance(bounds.step, Null) and not (
                isinstance(bounds.step, BindParameter)
                and bounds.step.unique
                and type(bounds.step.value) is int
                and bounds.step.value == 1
            ):
                raise exc.CompileError("Athena ARRAY slices support only step=None or step=1")
            start = "1" if isinstance(bounds.start, Null) else self.process(bounds.start, **kw)
            stop = (
                f"cardinality({array})"
                if isinstance(bounds.stop, Null)
                else self.process(bounds.stop, **kw)
            )
            start = f"greatest({start}, 1)"
            length = f"greatest(least({stop}, cardinality({array})) - {start} + 1, 0)"
            sql = f"slice({array}, {start}, {length})"
            return self._array_slice_step(sql, bounds.step, array_type, **kw)
        index_expression = binary.right
        if (
            isinstance(index_expression, BindParameter)
            and self._array_type_inspector.array_type(index_expression.type) is not None
        ):
            index_expression = index_expression._with_binary_element_type(types.Integer())
        index = self.process(index_expression, **kw)
        return f"element_at({array}, NULLIF(greatest({index}, 0), 0))"

    def _array_slice_step(self, sql, step, array_type, **kw):
        if isinstance(step, Null):
            return sql
        if isinstance(step, BindParameter):
            step = step._with_binary_element_type(_ArraySliceStepType())
        step_sql = self.process(step, **kw)
        failure = (
            "CAST(concat('Unsupported ARRAY slice step: ', "
            f"coalesce(CAST({step_sql} AS VARCHAR), 'NULL')) AS BIGINT)"
        )
        empty = (
            f"slice({sql}, 1, 0)"
            if _ArrayTypeInspector.has_unknown_element(array_type)
            else f"CAST(ARRAY[] AS {self._complex_dml_type(array_type)})"
        )
        return f"IF({step_sql} = 1, {sql}, slice({empty}, {failure}, 0))"

    def translate_select_structure(self, select_stmt, **kw):
        """Keep DISTINCT and ordering on native arrays before result serialization."""
        if (
            not self.stack
            and not kw.get("asfrom")
            and not select_stmt._annotations.get("_pyathena_array_result")
            and (select_stmt._distinct or select_stmt._order_by_clauses)
            and any(self._has_array_result(column) for column in select_stmt.selected_columns)
        ):
            return self._array_result_select(select_stmt)
        return select_stmt

    def visit_compound_select(self, cs, asfrom=False, compound_index=None, **kw):
        if (
            not self.stack
            and not asfrom
            and any(self._has_array_result(column) for column in cs.selected_columns)
        ):
            original_columns = list(cs.selected_columns)
            rendered = self.process(self._array_result_select(cs), **kw)
            self._result_columns = [
                entry._replace(objects=(*entry.objects, original))
                for entry, original in zip(self._result_columns, original_columns, strict=True)
            ]
            return rendered
        return super().visit_compound_select(cs, asfrom=asfrom, compound_index=compound_index, **kw)

    def _has_array_result(self, column):
        type_ = self._array_type_inspector.array_type(column.type)
        return type_ is not None and not _ArrayTypeInspector.has_unknown_element(type_)

    def _array_result_select(self, statement):
        if any(
            isinstance(column, TextClause)
            or (getattr(column, "is_literal", False) and column.name.rstrip().endswith("*"))
            for column in statement._all_selected_columns
        ):
            raise exc.CompileError(
                "Ordered, DISTINCT, and compound ARRAY results require explicit SELECT columns; "
                "use SQLAlchemy column expressions or literal_column() instead of text(), "
                "and select(table) instead of a wildcard"
            )
        if any(
            getattr(column, "is_literal", False)
            and not re.fullmatch(r'(?:[^\W\d]\w*|"(?:[^"]|"")+")', column.name)
            for column in statement._all_selected_columns
        ):
            raise exc.CompileError(
                "Literal SQL expressions in ordered, DISTINCT, and compound ARRAY results "
                "require an explicit label; use literal_column(...).label(...)"
            )
        columns = list(statement.selected_columns)
        inner = statement.order_by(None).limit(None).offset(None)
        ordering = []
        hidden: list[Any] = []
        label_resolve = (
            dict(statement.selected_columns.items())
            if isinstance(statement, CompoundSelect)
            else statement._compile_state_factory(statement, self)._label_resolve_dict[0]
        )

        def resolve_label(element: Any, **kw: Any) -> Any:
            if isinstance(element, _textual_label_reference):
                try:
                    return label_resolve[element.element]
                except KeyError as error:
                    raise exc.CompileError(
                        f"Can't resolve ARRAY ORDER BY label {element.element!r}"
                    ) from error
            if isinstance(element, _label_reference):
                return element.element
            return None

        clauses: list[Any] = []
        for clause in statement._order_by_clauses:
            if isinstance(clause, TextClause):
                try:
                    clauses.extend(TextClause(part) for part in _split_type_arguments(clause.text))
                except ValueError as error:
                    raise exc.CompileError(
                        "Textual ARRAY ordering must name selected columns; "
                        "use SQLAlchemy column expressions for other ordering"
                    ) from error
            else:
                clauses.append(clause)
        for clause in clauses:
            if isinstance(clause, TextClause):
                match = re.fullmatch(
                    r'\s*("(?:[^"]|"")+"|[\w]+)(?:\s+(ASC|DESC))?(?:\s+NULLS\s+(FIRST|LAST))?\s*',
                    clause.text,
                    re.IGNORECASE,
                )
                if match:
                    name, direction, nulls = match.groups()
                    quoted = name.startswith('"')
                    name = name[1:-1].replace('""', '"') if quoted else name
                    target = None
                    if not quoted and name.isdigit() and 1 <= int(name) <= len(columns):
                        target = columns[int(name) - 1]
                    elif name in statement.selected_columns:
                        target = statement.selected_columns[name]
                    if target is not None:
                        clause = target
                        if direction:
                            clause = clause.desc() if direction.upper() == "DESC" else clause.asc()
                        if nulls:
                            clause = (
                                clause.nulls_first()
                                if nulls.upper() == "FIRST"
                                else clause.nulls_last()
                            )
                if isinstance(clause, TextClause):
                    raise exc.CompileError(
                        "Textual ARRAY ordering must name selected columns; "
                        "use SQLAlchemy column expressions for other ordering"
                    )
            clause = visitors.replacement_traverse(clause, {}, resolve_label)
            modifiers = []
            while isinstance(clause, UnaryExpression) and clause.modifier in (
                operators.asc_op,
                operators.desc_op,
                operators.nulls_first_op,
                operators.nulls_last_op,
            ):
                modifiers.append(clause.modifier)
                clause = clause.element
            index = next((i for i, column in enumerate(columns) if column.compare(clause)), None)
            if (
                index is None
                and hasattr(inner, "add_columns")
                and not inner._distinct
                and not isinstance(clause, TextClause)
            ):
                name = f"_pyathena_order_{len(hidden)}"
                while name in statement.selected_columns:
                    name += "_"
                hidden.append(clause.label(name))
                index = len(columns) + len(hidden) - 1
            ordering.append((clause, index, modifiers))

        if hidden:
            inner = inner.add_columns(*hidden)
        source = inner.subquery()
        outer = select(*list(source.c)[: len(columns)])
        adapter = sql_util.ClauseAdapter(source)
        for clause, index, modifiers in ordering:
            expression = source.c[index] if index is not None else adapter.traverse(clause)
            if any(from_ is not source for from_ in expression._from_objects):
                raise exc.CompileError(
                    "DISTINCT and compound ARRAY ORDER BY expressions "
                    "must refer to selected columns"
                )
            for modifier in reversed(modifiers):
                expression = UnaryExpression(expression, modifier=modifier)
            outer = outer.order_by(expression)
        outer = outer.offset(statement._offset_clause)
        if statement._fetch_clause is not None:
            outer = outer.fetch(statement._fetch_clause, **statement._fetch_clause_options)
        else:
            outer = outer.limit(statement._limit_clause)
        return outer._annotate({"_pyathena_array_result": True})

    def visit_filter_func(self, fn: Function[Any], **kw: Any) -> str:
        """Compile Athena filter() function with lambda expressions.

        Supports syntax: filter(array_expr, lambda_expr)
        Example: filter(ARRAY[1, 2, 3], x -> x > 1)
        """
        if len(fn.clauses.clauses) != 2:
            raise exc.CompileError(
                f"filter() function expects exactly 2 arguments, got {len(fn.clauses.clauses)}"
            )

        array_expr = fn.clauses.clauses[0]
        lambda_expr = fn.clauses.clauses[1]

        # Process the array expression normally
        array_sql = self.process(array_expr, **kw)

        # Process lambda expression - handle string literals as lambda expressions
        if isinstance(lambda_expr, BindParameter) and isinstance(lambda_expr.value, str):
            # Handle string literal lambda expressions like 'x -> x > 0'
            lambda_sql = lambda_expr.value
        else:
            # Process as regular SQL expression
            lambda_sql = self.process(lambda_expr, **kw)

        return f"filter({array_sql}, {lambda_sql})"

    def visit_truediv_binary(self, binary, operator, **kw):
        """Render true division with explicit Athena numeric coercions."""
        left_type = binary.left.type
        right_type = binary.right.type

        if isinstance(left_type, types.Float) or isinstance(right_type, types.Float):
            division_type = get_double_type()()
            return (
                self.process(Cast(binary.left, division_type), **kw)
                + " / "
                + self.process(Cast(binary.right, division_type), **kw)
            )

        left_is_numeric = isinstance(left_type, types.Numeric)
        right_is_numeric = isinstance(right_type, types.Numeric)
        if left_is_numeric or right_is_numeric:
            division_type = binary.type
            return (
                self.process(Cast(binary.left, division_type), **kw)
                + " / "
                + self.process(Cast(binary.right, division_type), **kw)
            )

        if isinstance(left_type, types.Integer) and isinstance(right_type, types.Integer):
            return (
                self.process(binary.left, **kw)
                + " / "
                + self.process(Cast(binary.right, get_double_type()()), **kw)
            )

        return super().visit_truediv_binary(binary, operator, **kw)

    def visit_cast(self, cast: Cast[Any], **kwargs):
        if isinstance(cast.type, (types.ARRAY, AthenaMap, AthenaStruct)):
            type_clause = self._complex_dml_type(
                cast.type, implicit_bind=cast._annotations.get("_pyathena_array_bind", False)
            )
            return f"CAST({self.process(cast.clause, **kwargs)} AS {type_clause})"
        if (isinstance(cast.type, types.VARCHAR) and cast.type.length is None) or isinstance(
            cast.type, types.String
        ):
            type_clause = "VARCHAR"
        elif isinstance(cast.type, types.CHAR) and cast.type.length is None:
            type_clause = "CHAR"
        elif isinstance(cast.type, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            type_clause = "VARBINARY"
        elif hasattr(types, "DOUBLE") and isinstance(cast.type, types.DOUBLE):
            type_clause = "DOUBLE"
        elif isinstance(cast.type, (types.FLOAT, types.Float, types.REAL)):
            # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
            # In Athena, use float in DDL statements like CREATE TABLE
            # and real in SQL functions like SELECT CAST.
            type_clause = "REAL"
        else:
            type_clause = cast.typeclause._compiler_dispatch(self, **kwargs)
        return f"CAST({cast.clause._compiler_dispatch(self, **kwargs)} AS {type_clause})"

    def _complex_dml_type(self, type_, *, implicit_bind=False):
        if isinstance(type_, types.TypeDecorator):
            return self._complex_dml_type(
                self._array_type_inspector.decorator_impl(type_), implicit_bind=implicit_bind
            )
        if isinstance(type_, types.NullType):
            raise exc.CompileError("Bound ARRAY values require an explicit element type")
        if isinstance(type_, types.ARRAY):
            item = self._complex_dml_type(
                _ArrayTypeInspector.item_type(type_), implicit_bind=implicit_bind
            )
            return f"ARRAY({item})"
        if isinstance(type_, AthenaMap):
            return (
                f"MAP({self._complex_dml_type(type_.key_type, implicit_bind=implicit_bind)}, "
                f"{self._complex_dml_type(type_.value_type, implicit_bind=implicit_bind)})"
            )
        if isinstance(type_, AthenaStruct):
            fields = ", ".join(
                f"{self.preparer.quote(name)} "
                f"{self._complex_dml_type(field_type, implicit_bind=implicit_bind)}"
                for name, field_type in type_.fields.items()
            )
            return f"ROW({fields})"
        if isinstance(type_, types.String):
            return "VARCHAR"
        if isinstance(type_, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            return "VARBINARY"
        if isinstance(type_, getattr(types, "Double", get_double_type())):
            return "DOUBLE"
        if isinstance(type_, types.Float):
            return "REAL"
        if implicit_bind and isinstance(type_, types.Numeric) and type_.precision is None:
            raise exc.CompileError(
                "ARRAY decimal binds require explicit Numeric precision; "
                "specify precision and scale to avoid implicit rounding"
            )
        return self.dialect.type_compiler_instance.process(type_)

    def visit_athena_array_json_projection(self, expression, **kw):
        value = self.process(expression.element, **kw)
        encoded = self._array_json(value, expression.array_type)
        # An object envelope keeps SQL NULL and CSV null markers out of the transport.
        return f"json_format(CAST(MAP(ARRAY['_pyathena_array'], ARRAY[{encoded}]) AS JSON))"

    def _array_json(self, value, type_, depth=0):
        if isinstance(type_, types.TypeDecorator):
            return self._array_json(value, self._array_type_inspector.decorator_impl(type_), depth)
        # Each recursive value becomes JSON, including map keys and typed scalar leaves.
        variable = f"_pyathena_array_{depth}"
        if isinstance(type_, types.ARRAY):
            child = self._array_json(variable, _ArrayTypeInspector.item_type(type_), depth + 1)
            return f"CAST(transform({value}, {variable} -> {child}) AS JSON)"
        if isinstance(type_, AthenaMap):
            key = self._array_json(f"{variable}[1]", type_.key_type, depth + 1)
            item = self._array_json(f"{variable}[2]", type_.value_type, depth + 1)
            return (
                f"CAST(transform(map_entries({value}), {variable} -> ARRAY[{key}, {item}]) AS JSON)"
            )
        if isinstance(type_, AthenaStruct) and type_.fields:
            names = ", ".join(
                self.render_literal_value(name, types.String()) for name in type_.fields
            )
            fields = ", ".join(
                self._array_json(f"({value}).{self.preparer.quote(name)}", field_type, depth + 1)
                for name, field_type in type_.fields.items()
            )
            return (
                f"IF({value} IS NULL, CAST(NULL AS JSON), "
                f"CAST(MAP(ARRAY[{names}], ARRAY[{fields}]) AS JSON))"
            )
        if isinstance(type_, (types.JSON, types.NullType, AthenaStruct)):
            return f"CAST({value} AS JSON)"
        if isinstance(type_, (types.LargeBinary, types.BINARY, types.VARBINARY)):
            return f"CAST(to_hex({value}) AS JSON)"
        return f"CAST(CAST({value} AS VARCHAR) AS JSON)"

    def limit_clause(self, select: GenerativeSelect, **kw):
        text = []
        if select._offset_clause is not None:
            text.append(" OFFSET " + self.process(select._offset_clause, **kw))
        if select._limit_clause is not None:
            text.append(" LIMIT " + self.process(select._limit_clause, **kw))
        return "\n".join(text)

    def get_from_hint_text(self, table, text):
        return text

    def format_from_hint_text(self, sqltext, table, hint, iscrud):
        hint_upper = hint.upper()
        if (
            any(
                [
                    hint_upper.startswith("FOR TIMESTAMP AS OF"),
                    hint_upper.startswith("FOR SYSTEM_TIME AS OF"),
                    hint_upper.startswith("FOR VERSION AS OF"),
                    hint_upper.startswith("FOR SYSTEM_VERSION AS OF"),
                ]
            )
            and "AS" in sqltext
        ):
            _, alias = sqltext.split(" AS ", 1)
            return f"{table.original.fullname} {hint} AS {alias}"

        return f"{sqltext} {hint}"


class AthenaDDLCompiler(DDLCompiler):
    """DDL compiler for Amazon Athena CREATE TABLE and related statements.

    This compiler generates Athena-compatible DDL statements including support
    for Athena-specific table options:

    - External table creation (EXTERNAL keyword for Hive-style tables)
    - Iceberg table creation (managed tables with ACID support)
    - Amazon S3 Tables (Iceberg-backed, managed storage): set the connection
      ``catalog_name`` to ``s3tablescatalog/<table-bucket>`` and use the
      namespace as the table ``schema``. The LOCATION clause is omitted since
      storage is managed.
    - File formats: PARQUET, ORC, TEXTFILE, JSON, AVRO, etc.
    - Row formats with SerDe specifications
    - Compression settings for various file formats
    - Table locations in S3
    - Partitioning (both Hive-style and Iceberg transforms)
    - Bucketing/clustering for optimized queries

    The compiler uses backtick quoting for DDL identifiers (different from
    DML which uses double quotes) and handles Athena's reserved words.

    Example:
        A table created with this compiler might generate::

            CREATE EXTERNAL TABLE IF NOT EXISTS my_schema.my_table (
                id INT,
                name STRING
            )
            PARTITIONED BY (
                dt STRING
            )
            STORED AS PARQUET
            LOCATION 's3://my-bucket/my-table/'
            TBLPROPERTIES ('parquet.compress' = 'SNAPPY')

    See Also:
        AWS Athena CREATE TABLE:
        https://docs.aws.amazon.com/athena/latest/ug/create-table.html
    """

    @property
    def preparer(self) -> IdentifierPreparer:
        return self._preparer

    @preparer.setter
    def preparer(self, value: IdentifierPreparer):
        pass

    def __init__(
        self,
        dialect: AthenaDialect,
        statement: CreateTable,
        schema_translate_map: dict[str | None, str | None] | None = None,
        render_schema_translate: bool = False,
        compile_kwargs: dict[str, Any] | None = None,
    ):
        self._preparer = AthenaDDLIdentifierPreparer(dialect)
        super().__init__(
            dialect=dialect,
            statement=statement,
            render_schema_translate=render_schema_translate,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs or util.immutabledict(),
        )

    def _escape_comment(self, value: str) -> str:
        value = value.replace("\\", "\\\\").replace("'", r"\'")
        # DDL statements raise a KeyError if the placeholders aren't escaped
        if self.dialect.identifier_preparer._double_percents:
            value = value.replace("%", "%%")
        return f"'{value}'"

    def _get_comment_specification(self, comment: str) -> str:
        return f"COMMENT {self._escape_comment(comment)}"

    def _get_bucket_count(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str | None:
        if dialect_opts["bucket_count"]:
            bucket_count = dialect_opts["bucket_count"]
        elif connect_opts:
            bucket_count = connect_opts.get("bucket_count")
        else:
            bucket_count = None
        return cast(str, bucket_count) if bucket_count is not None else None

    def _get_file_format(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str | None:
        if dialect_opts["file_format"]:
            file_format = dialect_opts["file_format"]
        elif connect_opts:
            file_format = connect_opts.get("file_format")
        else:
            file_format = None
        return cast(str | None, file_format)

    def _get_file_format_specification(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str:
        file_format = self._get_file_format(dialect_opts, connect_opts)
        text = []
        if file_format:
            text.append(f"STORED AS {file_format}")
        return "\n".join(text)

    def _get_row_format(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str | None:
        if dialect_opts["row_format"]:
            row_format = dialect_opts["row_format"]
        elif connect_opts:
            row_format = connect_opts.get("row_format")
        else:
            row_format = None
        return cast(str | None, row_format)

    def _get_row_format_specification(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str:
        row_format = self._get_row_format(dialect_opts, connect_opts)
        text = []
        if row_format:
            text.append(f"ROW FORMAT {row_format}")
        return "\n".join(text)

    def _get_serde_properties(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str | dict[str, Any] | None:
        if dialect_opts["serdeproperties"]:
            serde_properties = dialect_opts["serdeproperties"]
        elif connect_opts:
            serde_properties = connect_opts.get("serdeproperties")
        else:
            serde_properties = None
        return cast(str | None, serde_properties)

    def _get_serde_properties_specification(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str:
        serde_properties = self._get_serde_properties(dialect_opts, connect_opts)
        text = []
        if serde_properties:
            text.append("WITH SERDEPROPERTIES (")
            if isinstance(serde_properties, dict):
                text.append(",\n".join([f"\t'{k}' = '{v}'" for k, v in serde_properties.items()]))
            else:
                text.append(serde_properties)
            text.append(")")
        return "\n".join(text)

    @staticmethod
    def _is_s3_tables_catalog(connect_opts: Mapping[str, Any]) -> bool:
        """Return whether the connection targets an Amazon S3 Tables catalog.

        S3 Tables are queried by setting the connection ``catalog_name`` to
        ``s3tablescatalog/<table-bucket>`` and using the namespace as the table
        ``schema`` (a two-part ``namespace.table`` identifier). Athena rejects a
        three-part ``catalog.namespace.table`` identifier in DDL, so the catalog
        must be selected at the connection level. Such tables use managed
        storage, so their CREATE TABLE statement must omit the LOCATION clause.

        Args:
            connect_opts: The dialect connection options.

        Returns:
            True if ``catalog_name`` names an S3 Tables catalog.
        """
        if not connect_opts:
            return False
        catalog = connect_opts.get("catalog_name") or ""
        # Athena resolves catalog names case-insensitively.
        return catalog.lower().startswith(S3_TABLES_CATALOG_PREFIX)

    def _is_iceberg_table(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> bool:
        """Return whether the table properties declare an Iceberg table.

        Args:
            dialect_opts: The table's ``awsathena_*`` dialect options.
            connect_opts: The dialect connection options.

        Returns:
            True if the rendered TBLPROPERTIES set ``table_type`` to Iceberg.
        """
        table_properties = self._get_table_properties_specification(
            dialect_opts, connect_opts
        ).lower()
        return ("table_type" in table_properties) and ("iceberg" in table_properties)

    def _validate_s3_tables_create_table(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> None:
        """Validate a CREATE TABLE compiled against an S3 Tables catalog.

        S3 Tables support only Iceberg tables on managed storage, so the table
        must declare ``table_type`` ICEBERG and must not specify a location.
        Raising here surfaces a clear client-side error instead of emitting DDL
        that Athena would reject.

        Args:
            dialect_opts: The table's ``awsathena_*`` dialect options.
            connect_opts: The dialect connection options.

        Raises:
            exc.CompileError: If the table is not Iceberg or specifies a location.
        """
        if not self._is_iceberg_table(dialect_opts, connect_opts):
            raise exc.CompileError(
                "S3 Tables support only Iceberg tables; specify the dialect keyword "
                "argument `awsathena_tblproperties={'table_type': 'ICEBERG'}`"
            )
        if dialect_opts["location"]:
            raise exc.CompileError(
                "S3 Tables use managed storage and do not accept a table location; "
                "remove the dialect keyword argument `awsathena_location`"
            )

    def _get_table_location(
        self, table: Table, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str | None:
        if dialect_opts["location"]:
            location = cast(str, dialect_opts["location"])
            location += "/" if not location.endswith("/") else ""
        elif connect_opts:
            base_location = (
                cast(str, connect_opts["location"])
                if "location" in connect_opts
                else cast(str, connect_opts.get("s3_staging_dir"))
            )
            schema = table.schema if table.schema else connect_opts["schema_name"]
            location = f"{base_location}{schema}/{table.name}/"
        else:
            location = None
        return location

    def _get_table_location_specification(
        self, table: Table, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str:
        location = self._get_table_location(table, dialect_opts, connect_opts)
        text = []
        if location:
            text.append(f"LOCATION '{location}'")
        else:
            if connect_opts:
                raise exc.CompileError(
                    "`location` or `s3_staging_dir` parameter is required in the connection string"
                )
            raise exc.CompileError(
                "The location of the table should be specified "
                "by the dialect keyword argument `awsathena_location`"
            )
        return "\n".join(text)

    def _get_table_properties(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> dict[str, str] | str | None:
        if dialect_opts["tblproperties"]:
            table_properties = cast(str, dialect_opts["tblproperties"])
        elif connect_opts:
            table_properties = cast(str, connect_opts.get("tblproperties"))
        else:
            table_properties = None
        return table_properties

    def _get_compression(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str | None:
        if dialect_opts["compression"]:
            compression = cast(str, dialect_opts["compression"])
        elif connect_opts:
            compression = cast(str, connect_opts.get("compression"))
        else:
            compression = None
        return compression

    def _get_table_properties_specification(
        self, dialect_opts: _DialectArgDict, connect_opts: Mapping[str, Any]
    ) -> str:
        properties = self._get_table_properties(dialect_opts, connect_opts)
        if properties:
            if isinstance(properties, dict):
                table_properties = [",\n".join([f"\t'{k}' = '{v}'" for k, v in properties.items()])]
            else:
                table_properties = [properties]
        else:
            table_properties = []

        compression = self._get_compression(dialect_opts, connect_opts)
        if compression:
            file_format = self._get_file_format(dialect_opts, connect_opts)
            row_format = self._get_row_format(dialect_opts, connect_opts)
            if file_format:
                if file_format == AthenaFileFormat.FILE_FORMAT_PARQUET:
                    table_properties.append(f"\t'parquet.compress' = '{compression}'")
                elif file_format == AthenaFileFormat.FILE_FORMAT_ORC:
                    table_properties.append(f"\t'orc.compress' = '{compression}'")
                else:
                    table_properties.append(f"\t'write.compress' = '{compression}'")
            elif row_format:
                if AthenaRowFormatSerde.is_parquet(row_format):
                    table_properties.append(f"\t'parquet.compress' = '{compression}'")
                elif AthenaRowFormatSerde.is_orc(row_format):
                    table_properties.append(f"\t'orc.compress' = '{compression}'")
                else:
                    table_properties.append(f"\t'write.compress' = '{compression}'")

        text = []
        if table_properties:
            text.append("TBLPROPERTIES (")
            text.append(",\n".join(table_properties))
            text.append(")")
        return "\n".join(text)

    def get_column_specification(self, column: Column[Any], **kwargs) -> str:
        if type(column.type) in [types.Integer, types.INTEGER, types.INT]:
            # https://docs.aws.amazon.com/athena/latest/ug/create-table.html
            # In Data Definition Language (DDL) queries like CREATE TABLE,
            # use the int keyword to represent an integer
            type_ = "INT"
        else:
            type_ = self.dialect.type_compiler.process(column.type, type_expression=column)
        text = [f"{self.preparer.format_column(column)} {type_}"]
        if column.comment:
            text.append(f"{self._get_comment_specification(column.comment)}")
        return " ".join(text)

    def visit_check_constraint(self, constraint: CheckConstraint, **kw: Any) -> str:
        return ""

    def visit_column_check_constraint(self, constraint: CheckConstraint, **kw: Any) -> str:
        return ""

    def visit_foreign_key_constraint(self, constraint: ForeignKeyConstraint, **kw: Any) -> str:
        return ""

    def visit_primary_key_constraint(self, constraint: PrimaryKeyConstraint, **kw: Any) -> str:
        return ""

    def visit_unique_constraint(self, constraint: UniqueConstraint, **kw: Any) -> str:
        return ""

    def _get_connect_option_partitions(self, connect_opts: Mapping[str, Any]) -> list[str]:
        if connect_opts:
            partition = cast(str, connect_opts.get("partition"))
            partitions = partition.split(",") if partition else []
        else:
            partitions = []
        return partitions

    def _get_connect_option_buckets(self, connect_opts: Mapping[str, Any]) -> list[str]:
        if connect_opts:
            bucket = cast(str, connect_opts.get("cluster"))
            buckets = bucket.split(",") if bucket else []
        else:
            buckets = []
        return buckets

    def _prepared_partitions(self, column: Column[Any]):
        # https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
        column_dialect_opts = column.dialect_options["awsathena"]
        partition_transform = column_dialect_opts["partition_transform"]

        column_name = self.preparer.format_column(column)
        transform_column = None

        partitions = []

        if partition_transform:
            if AthenaPartitionTransform.is_valid(partition_transform):
                if partition_transform == AthenaPartitionTransform.PARTITION_TRANSFORM_BUCKET:
                    bucket_count = column_dialect_opts["partition_transform_bucket_count"]
                    if bucket_count:
                        transform_column = f"{bucket_count}, {column_name}"
                elif partition_transform == AthenaPartitionTransform.PARTITION_TRANSFORM_TRUNCATE:
                    truncate_length = column_dialect_opts["partition_transform_truncate_length"]
                    if truncate_length:
                        transform_column = f"{truncate_length}, {column_name}"
                else:
                    transform_column = column_name

                if transform_column:
                    partitions.append(f"\t{partition_transform}({transform_column})")
        else:
            partitions.append(f"\t{column_name}")

        return partitions

    def _prepared_columns(
        self,
        table: Table,
        is_iceberg: bool,
        create_columns: list[CreateColumn],
        connect_opts: Mapping[str, Any],
    ) -> tuple[list[str], list[str], list[str]]:
        columns, partitions, buckets = [], [], []
        conn_partitions = self._get_connect_option_partitions(connect_opts)
        conn_buckets = self._get_connect_option_buckets(connect_opts)
        for create_column in create_columns:
            column = create_column.element
            column_dialect_opts = column.dialect_options["awsathena"]
            try:
                processed = self.process(create_column)
                if processed is not None:
                    if (
                        column_dialect_opts["partition"]
                        or column.name in conn_partitions
                        or f"{table.name}.{column.name}" in conn_partitions
                    ):
                        # https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
                        if is_iceberg:
                            partitions.extend(self._prepared_partitions(column=column))
                            columns.append(f"\t{processed}")
                        else:
                            partitions.append(f"\t{processed}")
                    else:
                        columns.append(f"\t{processed}")
                    if (
                        column_dialect_opts["cluster"]
                        or column.name in conn_buckets
                        or f"{table.name}.{column.name}" in conn_buckets
                    ):
                        buckets.append(f"\t{self.preparer.format_column(column)}")
            except exc.CompileError as e:
                raise exc.CompileError(
                    f"(in table '{table.description}', column '{column.name}'): {e.args[0]}"
                ) from e
        return columns, partitions, buckets

    def visit_create_table(self, create: CreateTable, **kwargs) -> str:
        table = create.element
        dialect_opts = table.dialect_options["awsathena"]
        dialect = cast("AthenaDialect", self.dialect)
        connect_opts = dialect._connect_options

        is_iceberg = self._is_iceberg_table(dialect_opts, connect_opts)

        # https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html
        text = ["\nCREATE TABLE"] if is_iceberg else ["\nCREATE EXTERNAL TABLE"]

        if create.if_not_exists:
            text.append("IF NOT EXISTS")
        text.append(self.preparer.format_table(table))
        text.append("(")
        text = [" ".join(text)]

        columns, partitions, buckets = self._prepared_columns(
            table, is_iceberg, create.columns, connect_opts
        )
        text.append(",\n".join(columns))
        text.append(")")

        if table.comment:
            text.append(self._get_comment_specification(table.comment))

        if partitions:
            text.append("PARTITIONED BY (")
            text.append(",\n".join(partitions))
            text.append(")")

        bucket_count = self._get_bucket_count(dialect_opts, connect_opts)
        if buckets and bucket_count:
            text.append("CLUSTERED BY (")
            text.append(",\n".join(buckets))
            text.append(f") INTO {bucket_count} BUCKETS")

        text.append(f"{self.post_create_table(table)}\n")
        return "\n".join(text)

    def post_create_table(self, table: Table) -> str:
        dialect_opts: _DialectArgDict = table.dialect_options["awsathena"]
        dialect = cast("AthenaDialect", self.dialect)
        connect_opts = dialect._connect_options
        if self._is_s3_tables_catalog(connect_opts):
            # S3 Tables are managed Iceberg tables: ROW FORMAT, SERDEPROPERTIES,
            # STORED AS, and LOCATION are not accepted, so emit only TBLPROPERTIES.
            self._validate_s3_tables_create_table(dialect_opts, connect_opts)
            text = [
                self._get_table_properties_specification(dialect_opts, connect_opts),
            ]
        else:
            text = [
                self._get_row_format_specification(dialect_opts, connect_opts),
                self._get_serde_properties_specification(dialect_opts, connect_opts),
                self._get_file_format_specification(dialect_opts, connect_opts),
                self._get_table_location_specification(table, dialect_opts, connect_opts),
                self._get_table_properties_specification(dialect_opts, connect_opts),
            ]
        return "\n".join([t for t in text if t])
