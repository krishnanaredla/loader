from pyspark import SparkConf
from pyspark.sql import SparkSession
import yaml
from typing import Dict
import pytest
import os


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
    return spark
