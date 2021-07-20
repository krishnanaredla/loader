import pytest
from dloader.dloader import *

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


def test_initialLoad(spark_test_session):
    spark = spark_test_session
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("TESTING")
    df = spark.read.option("header", "true").csv("fullLoad.csv")
    response = performFirstLoad(df, "/test/delta", "date_loaded")
    assert response == {"status": "Success", "message": ""}
