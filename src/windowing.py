"""Stage 4 - temporal windowing. Adds rolling mean/std and a W-cycle delta per
retained sensor. The window 'scope' decides whether temporal locality is kept:
  scope=['unit_nr']   -> partition-AWARE   (correct temporal neighbours)
  scope=['rand_part'] -> partition-UNAWARE (neighbours scrambled -> corrupted)."""
from pyspark.sql import functions as F, Window
from .config import W_WIN


def add_windows(df, keep, scope):
    w  = Window.partitionBy(*scope).orderBy("unit_nr", "time_cycles").rowsBetween(-(W_WIN - 1), 0)
    wl = Window.partitionBy(*scope).orderBy("unit_nr", "time_cycles")
    feats = list(keep)
    for c in keep:
        df = df.withColumn(f"{c}_rm", F.avg(c).over(w))
        df = df.withColumn(f"{c}_rs", F.coalesce(F.stddev(c).over(w), F.lit(0.0)))
        df = df.withColumn(f"{c}_dl", F.col(c) - F.coalesce(F.lag(c, W_WIN).over(wl), F.col(c)))
        feats += [f"{c}_rm", f"{c}_rs", f"{c}_dl"]
    return df, feats
