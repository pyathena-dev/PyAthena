(null_handling)=

# NULL and Empty String Handling

This section documents the behavior of NULL and empty string values across different cursor types in PyAthena.
Understanding this behavior is important for applications that need to distinguish between NULL (missing) values
and empty strings.

(null-csv-limitation)=

## CSV Format Limitation

When Athena executes a query, the results are stored as CSV files in S3. This CSV format has an inherent limitation
in how NULL values and empty strings are represented:

- **NULL values** are represented as unquoted empty fields: `,,`
- **Empty strings** are represented as quoted empty fields: `,"",`

```text
# Example CSV output from Athena
id,name,description
1,Alice,Hello
2,Bob,""
3,Charlie,

# Row 2: description is an empty string (quoted: "")
# Row 3: description is NULL (unquoted)
```

Most CSV parsers handle these differently:

- **Pandas** (used by `PandasCursor`): By default, treats both unquoted empty and quoted empty as missing values
- **PyArrow** (used by `ArrowCursor`): With `quoted_strings_can_be_null=False`, treats both as empty strings
- **Polars** (used by `PolarsCursor`): Correctly distinguishes quoted empty strings from unquoted NULL values
- **AthenaCSVReader** (used by `S3FSCursor`): Correctly distinguishes NULL (returns `None`) from empty strings

This means the ability to distinguish NULL from empty strings varies by cursor when reading CSV files.

## Cursor Behavior Comparison

The following table summarizes how different cursors handle NULL and empty string values,
based on actual testing with Athena:

| Cursor Type | Data Source | Empty String `''` | NULL Value | Distinguishes? |
| --- | --- | --- | --- | --- |
| `Cursor` (default) | Athena API | `''` | `None` | ✅ Yes |
| `DictCursor` | Athena API | `''` | `None` | ✅ Yes |
| `PandasCursor` | CSV file | `NaN` | `NaN` | ❌ No |
| `PandasCursor` + unload | Parquet file | `''` | `None` | ✅ Yes |
| `ArrowCursor` | CSV file | `''` | `''` | ❌ No |
| `ArrowCursor` + unload | Parquet file | `''` | `null` | ✅ Yes |
| `PolarsCursor` | CSV file | `''` | `null` | ✅ Yes |
| `PolarsCursor` + unload | Parquet file | `''` | `null` | ✅ Yes |
| `S3FSCursor` | CSV file | `''` | `None` | ✅ Yes |

```{note}
`PolarsCursor` and `S3FSCursor` are unique among the file-based cursors in that they can properly
distinguish NULL from empty strings even when reading CSV files. `PolarsCursor` uses Polars' CSV parser
which correctly interprets unquoted empty values as NULL, while `S3FSCursor` uses a custom
`AthenaCSVReader` that respects CSV quoting rules.
```

## Default Cursor (API-based)

The default `Cursor` and `DictCursor` fetch results directly from the Athena API,
which properly distinguishes between NULL and empty string values.

```python
from pyathena import connect

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2").cursor()

cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, ''),
            (2, CAST(NULL AS VARCHAR)),
            (3, 'hello')
    ) AS t(id, value)
""")

for row in cursor:
    print(f"id={row[0]}, value={repr(row[1])}, is_none={row[1] is None}")

# Output:
# id=1, value='', is_none=False     <- Empty string
# id=2, value=None, is_none=True    <- NULL
# id=3, value='hello', is_none=False
```

This is the most reliable cursor when distinguishing NULL from empty string is critical.

## PandasCursor Behavior

### Without Unload (CSV)

When using `PandasCursor` without the `unload` option, the cursor reads the CSV file
using pandas' `read_csv()`. **Both empty strings and NULL values are treated as NaN**.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor()

df = cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, '', 'empty_string'),
            (2, CAST(NULL AS VARCHAR), 'null_value'),
            (3, 'hello', 'normal_string')
    ) AS t(id, value, description)
""").as_pandas()

print(df)
#    id  value    description
# 0   1    NaN   empty_string   <- Empty string becomes NaN
# 1   2    NaN     null_value   <- NULL becomes NaN
# 2   3  hello  normal_string

print(df['value'].isna().tolist())
# [True, True, False]  <- Both empty string and NULL are NaN
```

```{note}
By default, PyAthena sets `keep_default_na=False` and `na_values=("",)` which means only
empty values are treated as NaN, while strings like "N/A", "NULL", "NA" are preserved as-is.
However, this still cannot distinguish between quoted empty strings and unquoted NULL values
in the CSV output.
```

### With Unload (Parquet) - Recommended

Using the `unload` option outputs results in Parquet format, which properly preserves
NULL semantics:

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PandasCursor).cursor(unload=True)

df = cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, '', 'empty_string'),
            (2, CAST(NULL AS VARCHAR), 'null_value'),
            (3, 'hello', 'normal_string')
    ) AS t(id, value, description)
""").as_pandas()

print(df)
#    id  value    description
# 0   1         empty_string   <- Empty string preserved
# 1   2   None    null_value   <- NULL is None
# 2   3  hello normal_string

print(df['value'].isna().tolist())
# [False, True, False]  <- Only NULL is NaN, empty string is not
```

## ArrowCursor Behavior

### Without Unload (CSV)

`ArrowCursor` uses PyArrow's CSV reader with `quoted_strings_can_be_null=False`,
which means **both NULL and empty strings become empty strings**:

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor()

table = cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, '', 'empty_string'),
            (2, CAST(NULL AS VARCHAR), 'null_value'),
            (3, 'hello', 'normal_string')
    ) AS t(id, value, description)
""").as_arrow()

value_col = table.column('value')
print(value_col.to_pylist())
# ['', '', 'hello']  <- Both empty string and NULL become ''

print(value_col.null_count)
# 0  <- No null values detected
```

### With Unload (Parquet) - Recommended

```python
from pyathena import connect
from pyathena.arrow.cursor import ArrowCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=ArrowCursor).cursor(unload=True)

table = cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, '', 'empty_string'),
            (2, CAST(NULL AS VARCHAR), 'null_value'),
            (3, 'hello', 'normal_string')
    ) AS t(id, value, description)
""").as_arrow()

value_col = table.column('value')
print(value_col.to_pylist())
# ['', None, 'hello']  <- NULL is properly None

print(value_col.null_count)
# 1  <- One null value correctly detected
```

## PolarsCursor Behavior

`PolarsCursor` is unique in that **it can distinguish NULL from empty strings even when reading CSV files**.
This is because Polars' CSV parser correctly interprets unquoted empty values as NULL.

### Without Unload (CSV)

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor()

df = cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, '', 'empty_string'),
            (2, CAST(NULL AS VARCHAR), 'null_value'),
            (3, 'hello', 'normal_string')
    ) AS t(id, value, description)
""").as_polars()

print(df)
# shape: (3, 3)
# ┌─────┬───────┬───────────────┐
# │ id  ┆ value ┆ description   │
# │ --- ┆ ---   ┆ ---           │
# │ i32 ┆ str   ┆ str           │
# ╞═════╪═══════╪═══════════════╡
# │ 1   ┆       ┆ empty_string  │  <- Empty string
# │ 2   ┆ null  ┆ null_value    │  <- NULL
# │ 3   ┆ hello ┆ normal_string │
# └─────┴───────┴───────────────┘

print(df['value'].is_null().to_list())
# [False, True, False]  <- Correctly distinguishes NULL

print(df['value'].to_list())
# ['', None, 'hello']  <- Empty string and NULL are different
```

### With Unload (Parquet)

The behavior is the same with unload, as Parquet also properly preserves NULL semantics:

```python
from pyathena import connect
from pyathena.polars.cursor import PolarsCursor

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=PolarsCursor).cursor(unload=True)

df = cursor.execute("SELECT * FROM your_table").as_polars()
# Same behavior as CSV - NULL and empty strings are properly distinguished
```

## S3FSCursor Behavior

`S3FSCursor` supports two CSV readers with different NULL handling behaviors:

- **AthenaCSVReader** (default): Properly distinguishes NULL from empty strings by respecting CSV quoting rules
- **DefaultCSVReader**: For backward compatibility; both NULL and empty strings become `None`

### AthenaCSVReader (Default)

By default, `S3FSCursor` uses `AthenaCSVReader` which correctly interprets unquoted empty
values as NULL and quoted empty values as empty strings.

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor

# AthenaCSVReader is used by default
cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor).cursor()

cursor.execute("""
    SELECT * FROM (
        VALUES
            (1, '', 'empty_string'),
            (2, CAST(NULL AS VARCHAR), 'null_value'),
            (3, 'hello', 'normal_string')
    ) AS t(id, value, description)
""")

for row in cursor:
    print(f"id={row[0]}, value={repr(row[1])}, is_none={row[1] is None}")

# Output:
# id=1, value='', is_none=False     <- Empty string preserved
# id=2, value=None, is_none=True    <- NULL is None
# id=3, value='hello', is_none=False
```

### DefaultCSVReader (Backward Compatibility)

If you need backward compatibility where both NULL and empty strings are treated as `None`,
you can explicitly specify `DefaultCSVReader`:

```python
from pyathena import connect
from pyathena.s3fs.cursor import S3FSCursor
from pyathena.s3fs.reader import DefaultCSVReader

cursor = connect(s3_staging_dir="s3://YOUR_S3_BUCKET/path/to/",
                 region_name="us-west-2",
                 cursor_class=S3FSCursor,
                 cursor_kwargs={"csv_reader": DefaultCSVReader}).cursor()

cursor.execute("SELECT '' AS empty_col, NULL AS null_col")
row = cursor.fetchone()
print(row)  # (None, None) - both become None
```

```{note}
`S3FSCursor` does not support the `unload` option. However, since `AthenaCSVReader` (the default)
can already distinguish NULL from empty strings when reading CSV files, this is not a limitation for NULL handling.
```

## Recommendations

Based on the cursor behaviors documented above:

1. **If you need to distinguish NULL from empty strings:**

   - Use the default `Cursor` or `DictCursor` (API-based, most reliable)
   - Or use `PolarsCursor` or `S3FSCursor` (works correctly even with CSV)
   - Or use any cursor with `unload=True` (Parquet format)

2. **If you need pandas DataFrame and NULL/empty string distinction:**

   - Use `PandasCursor` with `unload=True`

3. **If you need Arrow Table and NULL/empty string distinction:**

   - Use `ArrowCursor` with `unload=True`

4. **For data pipelines and ETL with DataFrame cursors:**

   - Prefer `PolarsCursor` (works correctly without unload)
   - Or use `unload=True` with any DataFrame cursor

5. **If performance is critical and NULL/empty distinction is not important:**

   - Any cursor works; CSV-based reading is generally faster for smaller datasets

## Summary Table

| Use Case | Recommended Cursor |
| --- | --- |
| Need NULL/empty distinction with tuples | `Cursor`, `DictCursor`, or `S3FSCursor` |
| Need pandas DataFrame with NULL/empty distinction | `PandasCursor` with `unload=True` |
| Need Arrow Table with NULL/empty distinction | `ArrowCursor` with `unload=True` |
| Need Polars DataFrame (any case) | `PolarsCursor` (works with or without unload) |
| Don't care about NULL/empty distinction | Any cursor (choose based on performance needs) |

## Unload Limitations

While the `unload` option solves the NULL/empty string issue for pandas and Arrow cursors,
it has some limitations:

- The UNLOAD statement has certain type restrictions (e.g., TIME type is not supported)
- Results are written to multiple files in parallel without guaranteed global sort order
- Column aliases are required for all SELECT expressions

See the {ref}`arrow-cursor` documentation for more details on unload limitations.

## Related Issues

This behavior is documented in response to the following GitHub issues:

- [#118](https://github.com/laughingman7743/PyAthena/issues/118): PandasCursor converting strings to NaN
- [#148](https://github.com/laughingman7743/PyAthena/issues/148): String NULL handling in pandas conversion
- [#168](https://github.com/laughingman7743/PyAthena/issues/168): Inconsistent NULL handling across data types
