from pyspark import SparkConf
from pyspark.sql import SparkSession
import yaml
from typing import Dict
import pytest
import os
import shutil


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

def pytest_configure():
    pytest.spark = None


@pytest.fixture(scope="session", autouse=True)
def spark_test_session():
    config = load_config()
    sparkconf = SparkConf().setAll(list(config["spark"]["config"].items()))
    spark = (
        SparkSession.builder.appName(config["spark"]["name"])
        .config(conf=sparkconf)
        .config("spark.jars", "delta-core_2.12-1.0.0.jar")
        .getOrCreate()
    )
    pytest.spark = spark
    return  spark
    #spark.stop()
    #shutil.rmtree(str("/test"))

