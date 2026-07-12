from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    max
)

from src.config import (
    RUL_DATA_FILE
)

def maximum_cycle_per_engine(df: DataFrame):
    """
    Returns

    engine_id | max_cycle
    """

    return (
        df
        .groupBy("engine_id")
        .agg(
            max("cycle").alias("max_cycle")
        )
    )

def create_rul(df: DataFrame):
    """
    Creates the Remaining Useful Life column.

    Formula:

    RUL = max_cycle - cycle
    """

    max_cycles = maximum_cycle_per_engine(df)

    df = (
        df.join(
            max_cycles,
            on="engine_id",
            how="left"
        )
    )

    df = df.withColumn(
        "RUL",
        col("max_cycle") - col("cycle")
    )

    return df.drop("max_cycle")

def validate_rul(df: DataFrame):
    """
    Prints min and max RUL.
    """

    from pyspark.sql.functions import min

    df.select(

        min("RUL").alias("Minimum RUL"),

        max("RUL").alias("Maximum RUL")

    ).show()

def engine_history(df: DataFrame, engine_id):
    """
    Returns all rows for one engine.
    """

    return (
        df
        .filter(
            col("engine_id") == engine_id
        )
        .orderBy("cycle")
    )

def save_rul_dataset(df: DataFrame):

    (
        df.write
        .mode("overwrite")
        .parquet(str(RUL_DATA_FILE))
    )

def load_rul_dataset(spark):

    return spark.read.parquet(
        str(RUL_DATA_FILE)
    )