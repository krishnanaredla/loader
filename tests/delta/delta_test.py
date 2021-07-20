import pytest
from delta.tables import *
import datetime
from dloader.dloader import *
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
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

expectedInitialData = [
    {
        "customerId": "3",
        "address": "current address for 3",
        "city": "chapel hil",
        "createdOn": "2020-01-01 4:26",
        "hashKey": "4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce",
        "hashData": "0e6be27212bd4e28e16826bc36b5c5fab70820bfe05bfdcc2a114a554e3b1f9b",
        "startDate": "2020-01-01 4:26",
        "endDate": None,
        "date_loaded": "2021-07-20",
    },
    {
        "customerId": "2",
        "address": "current address for 2",
        "city": "cary",
        "createdOn": "2020-01-01 7:34",
        "hashKey": "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35",
        "hashData": "fa1e24b2a865f8cae3461cc1dfd2867fcff4367874372b9f85c06cbc22d0b716",
        "startDate": "2020-01-01 7:34",
        "endDate": None,
        "date_loaded": "2021-07-20",
    },
    {
        "customerId": "1",
        "address": "old address for 1",
        "city": "durham",
        "createdOn": "2020-01-01 2:34",
        "hashKey": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        "hashData": "1b8c9703b187ae587450a42d549a8ae0469746e038589754c1c568bb329f4ef5",
        "startDate": "2020-01-01 2:34",
        "endDate": "2020-01-02 2:27",
        "date_loaded": "2021-07-20",
    },
    {
        "customerId": "1",
        "address": "current address for 1",
        "city": "durham",
        "createdOn": "2020-01-02 2:27",
        "hashKey": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        "hashData": "46dbc1691eecc26b6c01bb7b6fffe2f6148fb2fd1526ba28b60f90f57c7f30d6",
        "startDate": "2020-01-02 2:27",
        "endDate": None,
        "date_loaded": "2021-07-20",
    },
]

pkey = "customerId"
definingCol = "createdOn"
hashKeyCols = ["customerId"]
hashDataCols = ["address", "city"]
partitionCol = "date_loaded"
path = "/test/deltassza"


def test_initialLoad(spark_test_session):
    spark = spark_test_session
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("TESTING")
    df = spark.read.option("header", "true").csv("fullLoad.csv")
    windowBy = Window.partitionBy(f.col(pkey)).orderBy(f.col(definingCol))
    df = (
        df.withColumn(
            "hashKey",
            f.sha2(
                f.concat_ws("|", *map(lambda key_cols: f.col(key_cols), hashKeyCols)),
                256,
            ),
        )
        .withColumn(
            "hashData",
            f.sha2(
                f.concat_ws("|", *map(lambda key_cols: f.col(key_cols), hashDataCols)),
                256,
            ),
        )
        .withColumn("startDate", f.col(definingCol))
        .withColumn("endDate", f.lead(f.col(definingCol)).over(windowBy))
        .withColumn(
            str(partitionCol), f.lit(datetime.datetime.now().strftime("%Y-%m-%d"))
        )
    )
    response = performFirstLoad(df, path, "date_loaded")
    assert response == {"status": "Success", "message": ""}


def test_initialLoadData(spark_test_session):
    data = DeltaTable.forPath(spark_test_session, path).toDF()
    generatedDF = (
        data
        .filter(f.col('endDate').isNull())
        .select("customerId", "createdOn", "hashData", "startDate", "endDate")
        .orderBy(f.col("customerId").asc())
        #.limit(1)
        .collect()
    )
    expectedDF = (
        spark_test_session.createDataFrame(expectedInitialData)
        .filter(f.col('endDate').isNull())
        .select("customerId", "createdOn", "hashData", "startDate", "endDate")
        .orderBy(f.col("customerId").asc())
        #.limit(1)
        .collect()
    )
    assert generatedDF == expectedDF


def test_incLoad(spark_test_session):

    spark = spark_test_session
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("TESTING")
    df = spark.read.option("header", "true").csv("incload.csv")
    windowBy = Window.partitionBy(f.col(pkey)).orderBy(f.col(definingCol))
    df = (
        df.withColumn(
            "hashKey",
            f.sha2(
                f.concat_ws("|", *map(lambda key_cols: f.col(key_cols), hashKeyCols)),
                256,
            ),
        )
        .withColumn(
            "hashData",
            f.sha2(
                f.concat_ws("|", *map(lambda key_cols: f.col(key_cols), hashDataCols)),
                256,
            ),
        )
        .withColumn("startDate", f.col(definingCol))
        .withColumn("endDate", f.lead(f.col(definingCol)).over(windowBy))
        .withColumn(
            str(partitionCol), f.lit(datetime.datetime.now().strftime("%Y-%m-%d"))
        )
    )
    response = performDeltaLoad(df, path, "date_loaded")
    assert response == {"status": "Success", "message": ""}

