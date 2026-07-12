import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.config import (
    APP_NAME,
    MASTER,
    TRAIN_FILE,
    TEST_FILE,
    RUL_FILE
)

from src.constants import COLUMN_NAMES

def create_spark_session():
    """
    Creates and returns a SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(MASTER)
        .getOrCreate()

    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_raw_dataset(spark, file_path):
    """
    Reads one NASA dataset file.

    The raw NASA txt files contain whitespace-separated values
    and usually end with two empty columns.
    """

    df = (
        spark.read
        .option("sep", " ")
        .option("inferSchema", True)
        .csv(str(file_path))
    )

    return df

def remove_empty_columns(df):
    """
    Removes the empty columns that appear
    at the end of NASA dataset files.
    """

    valid_columns = []

    for column in df.columns:

        non_null = df.filter(col(column).isNotNull()).limit(1).count()

        if non_null > 0:
            valid_columns.append(column)

    return df.select(valid_columns)

def rename_columns(df):
    """
    Assigns meaningful column names.
    """

    if len(df.columns) != len(COLUMN_NAMES):
        raise ValueError(
            f"Expected {len(COLUMN_NAMES)} columns "
            f"but found {len(df.columns)}."
        )

    for old_name, new_name in zip(df.columns, COLUMN_NAMES):
        df = df.withColumnRenamed(old_name, new_name)

    return df

def load_training_data(spark):
    """
    Loads train_FD001.txt
    """

    df = read_raw_dataset(spark, TRAIN_FILE)

    df = remove_empty_columns(df)

    df = rename_columns(df)

    return df

def load_test_data(spark):
    """
    Loads test_FD001.txt
    """

    df = read_raw_dataset(spark, TEST_FILE)

    df = remove_empty_columns(df)

    df = rename_columns(df)

    return df

def load_rul_labels(spark):
    """
    Loads RUL_FD001.txt

    One value per engine.
    """

    df = (
        spark.read
        .option("inferSchema", True)
        .csv(str(RUL_FILE))
    )

    df = df.withColumnRenamed("_c0", "RUL")

    return df

def dataset_summary(df):
    """
    Prints basic information.
    """

    print("=" * 60)

    print("Rows:", df.count())

    print("Columns:", len(df.columns))

    print("=" * 60)

    df.printSchema()

    print("=" * 60)

    df.show(5, truncate=False)

def check_missing_values(df):
    """
    Counts missing values in every column.
    """

    from pyspark.sql.functions import isnan, when, count

    return df.select([
        count(
            when(
                col(c).isNull() | isnan(c),
                c
            )
        ).alias(c)
        for c in df.columns
    ])