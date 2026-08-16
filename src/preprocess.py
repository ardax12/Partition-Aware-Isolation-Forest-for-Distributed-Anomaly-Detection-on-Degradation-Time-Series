"""Stage 2/3 - RUL labels, drop constant sensors, operating-condition clustering
(Spark MLlib KMeans), condition-wise normalization, and deterministic keys."""
from pyspark.sql import functions as F, Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from .config import SENSORS, SETTINGS, N_COND, W_LABEL, SEED, N_PART


def add_labels(df):
    """RUL = max_cycle - cycle; label = 1 if RUL <= W_LABEL (degraded), else 0."""
    w = Window.partitionBy("unit_nr")
    df = df.withColumn("max_cycle", F.max("time_cycles").over(w))
    df = df.withColumn("RUL", F.col("max_cycle") - F.col("time_cycles"))
    df = df.withColumn("label", (F.col("RUL") <= F.lit(W_LABEL)).cast("int"))
    return df


def constant_sensors_to_drop(df):
    """Return the sensors that carry signal (std > 1e-6); the rest are constant."""
    stats = df.select(*[F.stddev(c).alias(c) for c in SENSORS]).collect()[0].asDict()
    return [c for c in SENSORS if stats[c] is not None and stats[c] > 1e-6]


def assign_conditions(df, subset):
    """Recover the operating regime of each row (KMeans on the 3 settings)."""
    k = N_COND[subset]
    if k <= 1:
        return df.withColumn("cond", F.lit(0))
    dfv = VectorAssembler(inputCols=SETTINGS, outputCol="set_vec").transform(df)
    model = KMeans(k=k, seed=SEED, featuresCol="set_vec", predictionCol="cond").fit(dfv)
    return model.transform(dfv).drop("set_vec")


def normalize_by_condition(df, keep):
    """Standardize each kept sensor within its operating condition (z-score)."""
    agg = []
    for c in keep:
        agg += [F.mean(c).over(Window.partitionBy("cond")).alias(f"{c}__m"),
                F.stddev(c).over(Window.partitionBy("cond")).alias(f"{c}__s")]
    df = df.select("*", *agg)
    for c in keep:
        df = df.withColumn(c, (F.col(c) - F.col(f"{c}__m")) /
                              (F.col(f"{c}__s") + F.lit(1e-9))).drop(f"{c}__m", f"{c}__s")
    return df


def add_keys(df):
    """Deterministic, machine-independent identifiers:
       row_id     unique per row within a subset (unit_nr, time_cycles)
       eng_part   engine assigned whole to a partition  (partition-AWARE)
       rand_part  row hashed to a partition             (partition-UNAWARE)"""
    df = df.withColumn("row_id", (F.col("unit_nr") * F.lit(100000) + F.col("time_cycles")).cast("long"))
    df = df.withColumn("rand_part", (F.abs(F.hash(F.col("row_id"), F.lit(SEED))) % N_PART))
    df = df.withColumn("eng_part", (F.col("unit_nr") % N_PART).cast("int"))
    return df


def preprocess(df, subset):
    """Full stage 2/3. Returns (dataframe, list of kept sensor columns)."""
    df = add_labels(df)
    keep = constant_sensors_to_drop(df)
    df = assign_conditions(df, subset)
    df = normalize_by_condition(df, keep)
    df = add_keys(df)
    return df, keep
