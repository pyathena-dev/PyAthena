import pyarrow as pa

from pyathena.arrow.util import to_column_info


def test_to_column_info():
    schema = pa.schema(
        [
            pa.field("col_boolean", pa.bool_()),
            pa.field("col_tinyint", pa.int32()),
            pa.field("col_smallint", pa.int32()),
            pa.field("col_int", pa.int32()),
            pa.field("col_bigint", pa.int64()),
            pa.field("col_float", pa.float32()),
            pa.field("col_double", pa.float64()),
            pa.field("col_string", pa.string()),
            pa.field("col_varchar", pa.string()),
            pa.field("col_timestamp", pa.timestamp("ns")),
            pa.field("col_date", pa.date32()),
            pa.field("col_binary", pa.binary()),
            pa.field("col_array", pa.list_(pa.field("array_element", pa.int32()))),
            pa.field("col_map", pa.map_(pa.int32(), pa.field("entries", pa.int32()))),
            pa.field(
                "col_struct",
                pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.int32())]),
            ),
            pa.field("col_decimal", pa.decimal128(10, 1)),
        ]
    )
    assert to_column_info(schema) == (
        {
            "Name": "col_boolean",
            "Nullable": "NULLABLE",
            "Precision": 0,
            "Scale": 0,
            "Type": "boolean",
        },
        {
            "Name": "col_tinyint",
            "Nullable": "NULLABLE",
            "Precision": 10,
            "Scale": 0,
            "Type": "integer",
        },
        {
            "Name": "col_smallint",
            "Nullable": "NULLABLE",
            "Precision": 10,
            "Scale": 0,
            "Type": "integer",
        },
        {
            "Name": "col_int",
            "Nullable": "NULLABLE",
            "Precision": 10,
            "Scale": 0,
            "Type": "integer",
        },
        {
            "Name": "col_bigint",
            "Nullable": "NULLABLE",
            "Precision": 19,
            "Scale": 0,
            "Type": "bigint",
        },
        {
            "Name": "col_float",
            "Nullable": "NULLABLE",
            "Precision": 17,
            "Scale": 0,
            "Type": "float",
        },
        {
            "Name": "col_double",
            "Nullable": "NULLABLE",
            "Precision": 17,
            "Scale": 0,
            "Type": "double",
        },
        {
            "Name": "col_string",
            "Nullable": "NULLABLE",
            "Precision": 2147483647,
            "Scale": 0,
            "Type": "varchar",
        },
        {
            "Name": "col_varchar",
            "Nullable": "NULLABLE",
            "Precision": 2147483647,
            "Scale": 0,
            "Type": "varchar",
        },
        {
            "Name": "col_timestamp",
            "Nullable": "NULLABLE",
            "Precision": 3,
            "Scale": 0,
            "Type": "timestamp",
        },
        {
            "Name": "col_date",
            "Nullable": "NULLABLE",
            "Precision": 0,
            "Scale": 0,
            "Type": "date",
        },
        {
            "Name": "col_binary",
            "Nullable": "NULLABLE",
            "Precision": 1073741824,
            "Scale": 0,
            "Type": "varbinary",
        },
        {
            "Name": "col_array",
            "Nullable": "NULLABLE",
            "Precision": 0,
            "Scale": 0,
            "Type": "array",
        },
        {
            "Name": "col_map",
            "Nullable": "NULLABLE",
            "Precision": 0,
            "Scale": 0,
            "Type": "map",
        },
        {
            "Name": "col_struct",
            "Nullable": "NULLABLE",
            "Precision": 0,
            "Scale": 0,
            "Type": "row",
        },
        {
            "Name": "col_decimal",
            "Nullable": "NULLABLE",
            "Precision": 10,
            "Scale": 1,
            "Type": "decimal",
        },
    )
