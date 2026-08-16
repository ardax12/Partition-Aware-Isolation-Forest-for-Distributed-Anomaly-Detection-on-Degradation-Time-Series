"""Spark session helper. Also forces Spark workers to use the driver's Python
so scikit-learn is importable inside applyInPandas UDFs."""
import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
from pyspark.sql import SparkSession


def get_spark(app_name="cmapss-iforest", master="local[*]"):
    spark = (SparkSession.builder.appName(app_name).master(master)
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
