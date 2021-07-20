import argparse
import ast
import concurrent.futures
import datetime
import json
import os
from functools import partial
from operator import is_not
import re
from typing import Dict, List
import uuid

import psycopg2
import psycopg2.extras
import yaml
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *


class Error(Exception):
    pass


class DLoaderException(Error):
    """
    Custom Exception class
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class DLoaderLogger(object):
    """
    log4j logger
    """

    def __init__(self, spark):
        self.log4jLogger = spark.sparkContext._jvm.org.apache.log4j

    def logger(self):
        component = "{}.{}".format(type(self).__module__, type(self).__name__)
        return self.log4jLogger.LogManager.getLogger(component)


class AuditManager:
    def __init__(
        self,
        db: str,
        user: str,
        password: str,
        host: str,
        port: int = 5432,
    ):
        """
        Class to access get and update information in the audit DB's
        """
        try:
            self.engine = psycopg2.connect(
                database=db, user=user, password=password, host=host, port=port
            )
            self.engine.autocommit = True
        except Exception as e:
            logger.error(str(e))
            raise DLoaderException("Unable to connect to postgres DB : {0}".format(e))

    def closeConnection(self):
        """
        Close connection to DB
        """
        self.engine.close()

    def getDataAsDict(self, query: str) -> List[Dict]:
        try:
            with self.engine.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query)
                data = json.dumps(cursor.fetchall(), default=str)
                cursor.close()
        except Exception as e:
            logger.error(str(e))
            raise DLoaderException("Unable to get data as dict from DB : {0}".format(e))
            return None
        return ast.literal_eval(data)

    def getSingleRecord(self, query: str) -> str:
        try:
            cursor = self.engine.cursor()
            cursor.execute(query)
            data = cursor.fetchone()[0]
            cursor.close()
            return data
        except Exception as e:
            logger.error(str(e))
            raise DLoaderException("Unable to get data as dict from DB : {0}".format(e))

    def getStepLogData(self) -> Dict:
        """
        Gets the required information for the Dloader
        to process data , gets data from step log table
        Args:
            None
        Return:
            Dict containing tables to process
        """
        query = """ SELECT file_process_id,
                        fp_id,
                        filename,
                        bucket_name
                    FROM   file_process_log
                    WHERE  file_process_id IN(SELECT file_process_id
                                            FROM   file_process_step_log
                                            WHERE  step_name = 'File Registration'
                                                    AND step_status = 'DONE'
                                                    AND step_end_ts > (SELECT
                                                        CASE
                                                        WHEN Max(step_start_ts) IS
                                                                NOT NULL THEN Max
                                                        (
                                                        step_start_ts)
                                                        ELSE
                                                        timestamp '1970-01-01 00:00:00'
                                                                END
                                                        FROM   file_process_step_log
                                                        WHERE  step_name = 'Data Loader'
                                                                AND step_status = 'DONE'))  
                """
        data = self.getDataAsDict(query)
        return data

    def getParition(self, fp_id: int) -> List:
        """
        Gets the partition info , required for the partition of the delta lake
        Args:
            fp_id            : fp_id
        Return:
            List of partition columns
        """
        partitionQuery = """select target_table_partition from file_pattern_detail where fp_id = {0}""".format(
            fp_id
        )
        partition = self.getSingleRecord(partitionQuery)
        return partition.split(",")

    def getSchema(self, fp_id: int) -> Dict:
        """
        Obtains the schema information which will be
        used to generate the spark schema
        Args:
            fp_id            : fp_id
        Return:
            Dict containing table columns info
        """
        # Identify the format
        formatquery = (
            """select file_format from file_pattern_detail where fp_id = {0}""".format(
                fp_id
            )
        )
        formattype = self.getDataAsDict(formatquery)[0]
        if formattype["file_format"] == "DELIMITED":
            schemaQuery = """
            select col_name,
            col_datatype,
            col_ordinal,
            primary_key_pos,
            is_defining_col,
            is_audit_col
            from delimited_col_detail where fp_detail_id = {}
            """.format(
                fp_id
            )
            data = self.getDataAsDict(schemaQuery)
            return data

    def updateStepLog(self, data: Dict) -> None:
        """
        Updates  process step log , at the end of each
        file processing
        Args :
                Data -- Dict
                Dictionary containing all the required values
                *all values must be passed*
        Return:
                None
        """
        step_payload = {
            **data,
            **{
                "step_end_ts": str(datetime.datetime.now()),
                "upsert_by": "DLoaderMS",
                "upsert_ts": str(datetime.datetime.now()),
            },
        }
        UpdateQuery = """
        UPDATE file_process_step_log
        SET    step_status = '{step_status}',
               step_status_detail = '{step_status_detail}',
               step_end_ts = timestamp '{step_end_ts}',
               upsert_by = '{upsert_by}',
               upsert_ts = timestamp '{upsert_ts}'
        WHERE  step_id = {step_id}
        """
        cursor = self.engine.cursor()
        try:
            cursor.execute(UpdateQuery.format(**step_payload))
        except Exception as e:
            raise DLoaderException(
                "Failed while inserting data into audit table {0}".format(e)
            )
        finally:
            cursor.close()

    def insertIntoStepLog(self, data: Dict) -> int:
        """
        Inserts data into process step log , at the start of each
        file processing
        Args :
                Data -- Dict
                Dictionary containing all the required values
                *all values must be passed*
        Return:
                Step id inserted
        """
        step_payload = {
            **data,
            **{
                "step_name": "Data Loader",
                "step_end_ts": str(datetime.datetime.now()),
                "upsert_by": "DLoaderMS",
                "upsert_ts": str(datetime.datetime.now()),
            },
        }

        insertQuery = """
        INSERT INTO file_process_step_log
                    (file_process_id,
                    step_name,
                    step_status,
                    step_status_detail,
                    step_start_ts,
                    step_end_ts,
                    upsert_by,
                    upsert_ts)
        VALUES    ( '{file_process_id}',
                    '{step_name}',
                    '{step_status}',
                    '{step_status_detail}',
                    timestamp '{step_start_ts}',
                    timestamp '{step_end_ts}',
                    '{upsert_by}',
                    timestamp '{upsert_ts}' ) 
        RETURNING step_id
        """
        cursor = self.engine.cursor()
        try:
            cursor.execute(insertQuery.format(**step_payload))
            step_id = cursor.fetchone()[0]
            return step_id
        except Exception as e:
            raise DLoaderException(
                "Failed while inserting data into audit table {0}".format(e)
            )
        finally:
            cursor.close()


def getDefiningCol(data: List[Dict]) -> List[str]:
    """
    Function to get the column/columns which define
    when the data has been modified , which will be used
    to obtain the latest record
    Args:
        data            : List containing , information about the table columns
    Return:
        List containing the defining column/columns
    """
    return list(
        filter(
            partial(is_not, None),
            list(
                map(
                    lambda x: x.get("col_name")
                    if x.get("is_defining_col") == "Y"
                    else None,
                    data,
                )
            ),
        )
    )


def getPrimaryKeys(data: List[Dict]) -> List[str]:
    """
    Function to get primary keys of a specified table
    Args:
        data            : List containing , information about the table columns
    Return:
        list containing primary keys
    """
    return list(
        filter(
            partial(is_not, None),
            list(
                map(
                    lambda x: x.get("col_name")
                    if x.get("col_ordinal") == x.get("primary_key_pos")
                    else None,
                    data,
                )
            ),
        )
    )


def getDataCols(data: List[Dict]) -> List[str]:
    """
    Function to get the data columns , which are used to generate the hash
    Args:
        data            : List containing , information about the table columns
    Return:
        List of columns to generate hash
    """
    return list(
        filter(
            partial(is_not, None),
            list(
                map(
                    lambda x: x.get("col_name")
                    if x.get("is_audit_col") == "N"
                    else None,
                    data,
                )
            ),
        )
    )


def generateSchema(data: List[Dict]) -> StructType:
    """
    Generates spark schema which will be used when reading the csv
    file, gets schema from DB table
    Args:
        data            : List containing , information about the table columns
    Return:
        None            : pyspark structtype
    """
    values = []
    dataTypes = {
        "ByteType": ByteType(),
        "ShortType": ShortType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "FloatType": FloatType(),
        "DoubleType": DoubleType(),
        "DecimalType": DecimalType(),
        "StringType": StringType(),
        "BinaryType": BinaryType(),
        "TimestampType": TimestampType(),
        "DateType": DateType(),
    }
    data = sorted(data, key=lambda i: i["col_ordinal"])
    result = any(
        map(
            lambda col: values.append(
                StructField(
                    col.get("col_name"), dataTypes.get(col.get("col_datatype")), True
                )
            ),
            data,
        )
    )
    return StructType(values)


def performFirstLoad(
    df: DataFrame, path: str, partitionCol: str = "date_loaded"
) -> Dict:
    """
    Function to perform the initial load into delta table
    Args:
        df              : pyspark DataFrame
        path            : delta table path
        partitionCol    : partition column
    Return:
        Job Status as Dict
    """
    try:
        (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .partitionBy(partitionCol)
            .save(path)
        )
        logger.info("Delta table created")
        return {"status": "Success", "message": ""}
    except Exception as e:
        logger.error(
            "Failed while performing the Initial load for {0} error : {1}".format(
                path, str(e)[:100]
            )
        )
        return {"status": "Failed", "message": str(e)}
        # raise DLoaderException(
        #    "Failed while loading the initial data into delta table : {0}".format(e)
        # )


def performDeltaLoad(df: DataFrame, path: str, pkey: str) -> Dict:
    """
    Function to perform the initial load into delta table
    Args:
        df              : pyspark DataFrame
        path            : delta table path
        pkey            : primary column
    Return:
        Job Status as Dict
    """
    try:
        oldData = DeltaTable.forPath(spark, path)
        dataToInsert = (
            df.alias("updates")
            .join(oldData.toDF().alias("oldData"), pkey)
            .where("oldData.endDate is null  AND updates.hashData <> oldData.hashData")
        )
        stagedUpdates = dataToInsert.selectExpr("NULL as mergeKey", "updates.*").union(
            df.alias("updates").selectExpr("updates.{0} as mergeKey".format(pkey), "*")
        )
        logger.info("Upsert Started")
        oldData.alias("oldData").merge(
            stagedUpdates.alias("staged_updates"),
            "oldData.endDate is null and oldData.{0} = mergeKey".format(pkey),
        ).whenMatchedUpdate(
            condition=" oldData.hashData <> staged_updates.hashData",
            set={"endDate": "staged_updates.startDate"},
        ).whenNotMatchedInsert(
            values={
                "{0}".format(str(col_name)): "staged_updates.{0}".format(str(col_name))
                for col_name in stagedUpdates.columns
                if col_name not in "mergeKey"
            }
        ).execute()
        logger.info("Upsert Completed")
        return {"status": "Success", "message": ""}
    except Exception as e:
        logger.error(
            "Failed while performing the Increamental load for {0} error : {1}".format(
                path, str(e)[:100]
            )
        )
        return {"status": "Failed", "message": str(e)}
        # raise DLoaderException(
        #    "Failed while loading the incremental data into delta table : {0}".format(e)
        # )


def processData(
    filePath: str,
    path: str,
    fileSchema: Dict,
    pkey: str,
    definingCol: str,
    hashKeyCols: List[str],
    hashDataCols: List[str],
    partitionCol: str = "date_loaded",
) -> Dict:
    """
    Data loader main function to process the incoming data,
    creates required columns
    Args:
        filePath            : S3 file location for input file
        path                : delta table path
        fileSchema          : input file schema loaded from db
        pkey                : primary key
        definingCol         : defining column
        hashKeyCols         : list of key columns which needs to be hashed
        hashDataCols        : list of data columns which needs to be hashed
        partitionCol        : partition Column
    Returns:
        File Status
    """
    try:
        schema = generateSchema(fileSchema)
        logger.info("Loading data from input file")
        df = (
            spark.read.format("csv")
            .option("header", True)
            .schema(schema)
            .load(filePath)
        )
        count = df.count()
        windowBy = Window.partitionBy(f.col(pkey)).orderBy(f.col(definingCol))
        logger.info("Generating the required Audit Columns")
        df = (
            df.withColumn(
                "hashKey",
                f.sha2(
                    f.concat_ws(
                        "|", *map(lambda key_cols: f.col(key_cols), hashKeyCols)
                    ),
                    256,
                ),
            )
            .withColumn(
                "hashData",
                f.sha2(
                    f.concat_ws(
                        "|", *map(lambda key_cols: f.col(key_cols), hashDataCols)
                    ),
                    256,
                ),
            )
            .withColumn("startDate", f.col(definingCol))
            .withColumn("endDate", f.lead(f.col(definingCol)).over(windowBy))
            .withColumn(
                str(partitionCol), f.lit(datetime.datetime.now().strftime("%Y-%m-%d"))
            )
        )
        if not DeltaTable.isDeltaTable(spark, path):
            logger.info("Performing the initial load")
            status = performFirstLoad(df, path, partitionCol)
        else:
            logger.info("Performing the incremental load")
            status = performDeltaLoad(df, path, pkey)
        return {**status, **{"count": count}}
    except Exception as e:
        logger.error(
            "Issue while processing the {0} error : {1}".format(filePath, str(e)[:100])
        )
        return {"status": "Failed", "message": str(e)[:100], "count": 0}
        # raise DLoaderException("Failed while processing the data :{0}".format(e))


def processFile(fileMeta: Dict):
    try:
        start_time = str(datetime.datetime.now())
        adt = AuditManager(
            config.get("Audit").get("database"),
            config.get("Audit").get("user"),
            config.get("Audit").get("password"),
            config.get("Audit").get("host"),
            config.get("Audit").get("port"),
        )
        fp_id = fileMeta.get("fp_id")
        file_process_id = fileMeta.get("file_process_id")
        step_id = adt.insertIntoStepLog(
            {
                "file_process_id": file_process_id,
                "step_status": "IN-PROGRESS",
                "step_status_detail": "running",
                "step_start_ts": start_time,
            }
        )
        filePath = "".join(
            ["s3a://", fileMeta.get("bucket_name"), "/", fileMeta.get("filename")]
        )
        path = "".join(
            [
                "s3a://",
                fileMeta.get("bucket_name").replace("landing", "raw"),
                "/",
                str(re.sub(r"_\d+.csv", "", fileMeta.get("filename").split("/")[-1])),
                ".parquet",
            ]
        )
        fileSchema = adt.getSchema(fp_id)
        pkey = getPrimaryKeys(fileSchema)[0]
        definingCol = getDefiningCol(fileSchema)[0]
        dataKeys = getDataCols(fileSchema)
        hashKeyCols = [pkey]
        hashDataCols = dataKeys
        partitionCol = adt.getParition(fp_id)[0]
        status = processData(
            filePath,
            path,
            fileSchema,
            pkey,
            definingCol,
            hashKeyCols,
            hashDataCols,
            partitionCol,
        )
        end_time = str(datetime.datetime.now())
        if status.get("status") == "Failed":
            adt.updateStepLog(
                {
                    "step_id": step_id,
                    "step_status": "ERROR",
                    "step_status_detail": status.get("message"),
                }
            )
            adt.closeConnection()
            return {
                **status,
                **{
                    "file_process_id": file_process_id,
                    "start": start_time,
                    "end": end_time,
                },
            }
        adt.updateStepLog(
            {
                "step_id": step_id,
                "step_status": "DONE",
                "step_status_detail": "Completed Successfully",
            }
        )
        adt.closeConnection()
        return {
            **status,
            **{
                "file_process_id": file_process_id,
                "start": start_time,
                "end": end_time,
            },
        }
    except Exception as e:
        logger.info(str(e))
        adt.updateStepLog(
            {
                "step_id": step_id,
                "step_status": "ERROR",
                "step_status_detail": str(e),
            }
        )
        adt.closeConnection()
        return {
            "file_process_id": file_process_id,
            "status": "Failed",
            "message": str(e),
            "start": start_time,
            "end": str(datetime.datetime.now()),
            "count": 0,
        }
        # raise DLoaderException("Failed while processing the file : {0}".format(e))


def main() -> None:
    """
    Main driving function for data loader,
    gets the info about the tables to process from
    getStepLogData and submitts parallel jobs (n = 2 default)
    to process each file individually
    """
    jobStatus = list()
    adt = AuditManager(
        config.get("Audit").get("database"),
        config.get("Audit").get("user"),
        config.get("Audit").get("password"),
        config.get("Audit").get("host"),
        config.get("Audit").get("port"),
    )
    jobMeta = adt.getStepLogData()
    adt.closeConnection()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.get("spark").get("parallelJobs", 2)
    ) as executor:
        spark_jobs = {
            executor.submit(processFile, fileMeta): fileMeta for fileMeta in jobMeta
        }
        for status in concurrent.futures.as_completed(spark_jobs):
            fileStatus = status.result()
            jobStatus.append(fileStatus)
    logger.info(jobStatus)


def load_config(region: str = "disc", path: str = None) -> Dict:
    if not path:
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "resources/config.yaml")
        )
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)
            raise FileNotFoundError(exc)
    return config


parser = argparse.ArgumentParser(description="Data Loader")
parser.add_argument("--config", "-c", required=False, help="Config yaml location")
args = parser.parse_args()


if __name__ == "__main__":
    try:
        config = load_config(args.config)
        sparkconf = SparkConf().setAll(list(config["spark"]["config"].items()))
        spark = (
            SparkSession.builder.appName(config["spark"]["name"])
            .config(conf=sparkconf)
            .getOrCreate()
        )
        spark.sparkContext.addPyFile("delta-core_2.12-1.0.0.jar")
        from delta.tables import *

        logger = DLoaderLogger(spark).logger()
        logger.info("Starting the Reltio Stream job")
        main()
    except Exception as e:
        raise DLoaderException("Failed while initiated the job {0}".format(e))
