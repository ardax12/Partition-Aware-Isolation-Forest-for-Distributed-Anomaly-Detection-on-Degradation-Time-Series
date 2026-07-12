from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

from src.config import (
    CLEAN_DATA_FILE
)

from src.constants import (
    FEATURE_COLUMNS
)

def dataset_shape(df: DataFrame):
    """
    Returns (rows, columns)
    """

    return df.count(), len(df.columns)

def print_dataset_info(df: DataFrame):
    """
    Prints basic dataset information.
    """

    rows, cols = dataset_shape(df)

    print("=" * 60)
    print(f"Rows    : {rows}")
    print(f"Columns : {cols}")
    print("=" * 60)

    df.printSchema()

    print("=" * 60)

def missing_values(df: DataFrame):
    """
    Returns missing values for every column.
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


def duplicate_count(df: DataFrame):
    """
    Counts duplicate rows.
    """

    total = df.count()

    unique = df.dropDuplicates().count()

    return total - unique

def duplicate_count(df: DataFrame):
    """
    Counts duplicate rows.
    """

    total = df.count()

    unique = df.dropDuplicates().count()

    return total - unique


def remove_duplicates(df: DataFrame):
    """
    Removes duplicate rows.
    """

    return df.dropDuplicates()

def describe_statistics(df: DataFrame):
    """
    Prints statistics.
    """

    df.describe().show()

def number_of_engines(df: DataFrame):
    """
    Number of engines.
    """

    return df.select("engine_id").distinct().count()


def engine_cycles(df: DataFrame):
    """
    Maximum cycle for each engine.
    """

    from pyspark.sql.functions import max

    return (
        df.groupBy("engine_id")
        .agg(max("cycle").alias("max_cycle"))
        .orderBy("engine_id")
    )

def create_feature_vector(df: DataFrame):
    """
    Creates Spark ML feature vector.
    """

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features"
    )

    return assembler.transform(df)

def save_clean_data(df: DataFrame):
    """
    Saves cleaned dataframe.
    """

    (
        df.write
        .mode("overwrite")
        .parquet(str(CLEAN_DATA_FILE))
    )

def load_clean_data(spark):
    """
    Loads processed parquet.
    """

    return spark.read.parquet(str(CLEAN_DATA_FILE))
