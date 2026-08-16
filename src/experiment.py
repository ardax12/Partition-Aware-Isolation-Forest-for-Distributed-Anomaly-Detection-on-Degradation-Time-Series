"""Stage 5c - run the four configurations on one subset (or all subsets).
  C1 single-node reference      (instantaneous features)
  C2 naive distributed          (random partitions, instantaneous)
  C3 windowed, partition-unaware (random partitions, corrupted windows)  [ablation]
  C4 proposed partition-aware    (engine partitions, correct windows)"""
from pyspark.sql import functions as F
from .ingest import ingest
from .preprocess import preprocess
from .windowing import add_windows
from .detector import score_per_partition, score_single_node
from .evaluate import evaluate
from .config import SUBSETS


def run_subset(spark, subset):
    df = ingest(spark, subset)
    df, keep = preprocess(df, subset)
    df = df.cache(); df.count()

    df_aw, feats_aw = add_windows(df, keep, ["unit_nr"])     # partition-aware
    df_un, feats_un = add_windows(df, keep, ["rand_part"])   # partition-unaware
    df_aw = df_aw.cache(); df_un = df_un.cache()

    cont = float(df.select(F.mean("label")).first()[0])
    labels = {r["row_id"]: r["label"] for r in df.select("row_id", "label").collect()}
    pdf = df.select("row_id", "label", *keep).toPandas()

    c1 = evaluate(score_single_node(pdf, keep, cont), labels)
    c2 = evaluate(score_per_partition(df, keep, cont, "rand_part"), labels)
    c3 = evaluate(score_per_partition(df_un, feats_un, cont, "rand_part"), labels)
    c4 = evaluate(score_per_partition(df_aw, feats_aw, cont, "eng_part"), labels)

    df_aw.unpersist(); df_un.unpersist(); df.unpersist()
    return {"C1_ref": c1, "C2_naive": c2, "C3_win_unaware": c3, "C4_proposed": c4,
            "keep_sensors": keep, "n_win_feats": len(feats_aw)}


def run_all(spark, subsets=SUBSETS):
    return {s: run_subset(spark, s) for s in subsets}
