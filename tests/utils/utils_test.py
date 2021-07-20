from dloader.dloader import *
from pyspark.sql.types import *

data = [
    {
        "col_name": "customerId",
        "col_datatype": "IntegerType",
        "col_ordinal": 0,
        "primary_key_pos": 0,
        "is_defining_col": "N",
        "is_audit_col": "Y",
    },
    {
        "col_name": "address",
        "col_datatype": "StringType",
        "col_ordinal": 1,
        "primary_key_pos": 0,
        "is_defining_col": "N",
        "is_audit_col": "N",
    },
    {
        "col_name": "city",
        "col_datatype": "StringType",
        "col_ordinal": 2,
        "primary_key_pos": 0,
        "is_defining_col": "N",
        "is_audit_col": "N",
    },
    {
        "col_name": "createdOn",
        "col_datatype": "TimestampType",
        "col_ordinal": 3,
        "primary_key_pos": 0,
        "is_defining_col": "Y",
        "is_audit_col": "Y",
    },
]


def test_getDefiningCol():
    Result = getDefiningCol(data)
    expectedResult = ["createdOn"]
    assert Result == expectedResult


def test_getPrimaryKeys():
    Result = getPrimaryKeys(data)
    expectedResult = ["customerId"]
    assert Result == expectedResult


def test_getDataCols():
    Result = getDataCols(data)
    expectedResult = ["address", "city"]
    assert Result == expectedResult


def test_generateSchema():
    Result = generateSchema(data)
    expectedResult = StructType(
        [
            StructField("customerId", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("createdOn", TimestampType(), True),
        ]
    )
    assert Result == expectedResult
