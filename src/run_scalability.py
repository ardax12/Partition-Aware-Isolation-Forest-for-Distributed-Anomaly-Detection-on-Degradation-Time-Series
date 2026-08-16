"""
Scalability driver for the partition-aware Isolation Forest.

Measures end-to-end throughput (rows/second) of the windowed scoring stage.
Designed to be run repeatedly at different worker counts on the Docker Compose
cluster (see ../docker), collecting one CSV row per run; plot_scal.py then turns
the CSV into figures/fig_scalability.png for the paper.

Environment variables:
  SPARK_MASTER  Spark master URL (default 'local[*]'; on the cluster use
                'spark://spark-master:7077')
  WORKERS       Label recorded in the CSV for this run (e.g. '1','2','4','8')
  DATA_DIR      Directory holding the raw C-MAPSS *.txt files (default ./CMAPSSData)
  OUT_DIR       Output directory (default ./results)

Typical cluster sweep (from ../docker):
  for n in 1 2 4 8; do
    docker compose up -d --scale spark-worker=$n
    docker compose exec spark-master \
      env WORKERS=$n SPARK_MASTER=spark://spark-master:7077 \
      spark-submit --master spark://spark-master:7077 /work/code/scalability.py
  done
  python3 code/plot_scal.py
"""
import os, sys, csv, time, numpy as np, pandas as pd
# Ensure Spark workers use this same interpreter so scikit-learn is importable
# inside the applyInPandas UDF (avoids "No module named 'sklearn'" on workers).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from sklearn.ensemble import IsolationForest

SEED, W_WIN = 42, 20
MASTER  = os.environ.get("SPARK_MASTER", "local[*]")
WORKERS = os.environ.get("WORKERS", "local")
DATA    = os.environ.get("DATA_DIR", "./data/raw")
OUT     = os.environ.get("OUT_DIR", "./results"); os.makedirs(OUT, exist_ok=True)
SENSORS = [f"s_{i}" for i in range(1, 22)]
COLS = ["unit_nr", "time_cycles"] + [f"setting_{i}" for i in range(1, 4)] + SENSORS

spark = (SparkSession.builder.appName("cmapss-scalability").master(MASTER)
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---- ingest all four subsets ----
def load(subset):
    raw = spark.read.text(f"{DATA}/train_{subset}.txt")
    arr = F.split(F.trim(F.col("value")), r"\s+")
    return raw.select(*[arr.getItem(i).cast("double").alias(c) for i, c in enumerate(COLS)]) \
              .withColumn("uid", F.concat_ws("_", F.lit(subset), F.col("unit_nr").cast("int")))

df = load("FD001")
for s in ["FD002", "FD003", "FD004"]:
    df = df.unionByName(load(s))

# ---- partition-aware windowing (correct temporal neighbours per trajectory) ----
stats = df.select(*[F.stddev(c).alias(c) for c in SENSORS]).collect()[0].asDict()
keep = [c for c in SENSORS if stats[c] and stats[c] > 1e-6]
w  = Window.partitionBy("uid").orderBy("time_cycles").rowsBetween(-(W_WIN - 1), 0)
wl = Window.partitionBy("uid").orderBy("time_cycles")
feats = list(keep)
for c in keep:
    df = df.withColumn(f"{c}_rm", F.avg(c).over(w))
    df = df.withColumn(f"{c}_dl", F.col(c) - F.coalesce(F.lag(c, W_WIN).over(wl), F.col(c)))
    feats += [f"{c}_rm", f"{c}_dl"]
df = df.select("uid", *feats).cache()
N = df.count()

schema = StructType([StructField("k", LongType()), StructField("s", DoubleType())])
def score(pdf):
    X = np.nan_to_num(pdf[feats].to_numpy())
    clf = IsolationForest(n_estimators=100, random_state=SEED, n_jobs=1).fit(X)
    return pd.DataFrame({"k": range(len(X)), "s": -clf.score_samples(X)})

# ---- timed scoring pass (best of 3) ----
times = []
for _ in range(3):
    t0 = time.time()
    df.groupBy("uid").applyInPandas(score, schema=schema).count()
    times.append(time.time() - t0)
best = min(times); thr = int(N / best)
print(f"[workers={WORKERS}] rows={N} time={best:.2f}s throughput={thr} rows/s")

# ---- append one row to the sweep CSV ----
path = f"{OUT}/scalability.csv"
new = not os.path.exists(path)
with open(path, "a", newline="") as f:
    wtr = csv.writer(f)
    if new:
        wtr.writerow(["workers", "rows", "time_s", "throughput_rows_s"])
    wtr.writerow([WORKERS, N, round(best, 2), thr])
spark.stop()
