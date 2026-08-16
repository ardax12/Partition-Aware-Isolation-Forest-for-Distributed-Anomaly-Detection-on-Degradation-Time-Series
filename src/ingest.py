"""Stage 1 - ingest raw C-MAPSS text into a typed Spark DataFrame and Parquet."""
from pyspark.sql import functions as F
from .config import COLS, DATA_RAW, PARQUET


def ingest(spark, subset):
    """Read train_<subset>.txt, assign the schema, and persist to Parquet."""
    raw = spark.read.text(str(DATA_RAW / f"train_{subset}.txt"))
    arr = F.split(F.trim(F.col("value")), r"\s+")
    df = raw.select(*[arr.getItem(i).cast("double").alias(c) for i, c in enumerate(COLS)])
    df = (df.withColumn("unit_nr", F.col("unit_nr").cast("long"))
            .withColumn("time_cycles", F.col("time_cycles").cast("long")))
    pq = str(PARQUET / subset)
    df.write.mode("overwrite").parquet(pq)
    return spark.read.parquet(pq)
