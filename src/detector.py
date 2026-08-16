"""Stage 5a - the detector. An Isolation Forest is trained per Spark partition
via applyInPandas (Spark MLlib has no native Isolation Forest). Rows are sorted
by row_id inside each partition so results are exactly reproducible."""
import numpy as np, pandas as pd
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from sklearn.ensemble import IsolationForest
from .config import SEED


def _scores(X, contamination):
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    clf = IsolationForest(n_estimators=100, contamination=contamination,
                          random_state=SEED, n_jobs=1)
    clf.fit(X)
    return -clf.score_samples(X)          # higher = more anomalous


def score_per_partition(df, feat_cols, contamination, part_col):
    """Distributed: one forest per partition group (part_col). Returns {row_id: score}."""
    schema = StructType([StructField("row_id", LongType()), StructField("score", DoubleType())])

    def _udf(pdf):
        pdf = pdf.sort_values("row_id")
        s = _scores(pdf[feat_cols].to_numpy(), contamination)
        return pd.DataFrame({"row_id": pdf["row_id"].to_numpy(), "score": s})

    scored = (df.select("row_id", part_col, *feat_cols)
                .groupBy(part_col).applyInPandas(_udf, schema=schema))
    return {r["row_id"]: r["score"] for r in scored.collect()}


def score_single_node(pdf, feat_cols, contamination):
    """C1 reference: a single Isolation Forest on the full dataset. Returns {row_id: score}."""
    pdf = pdf.sort_values("row_id")
    s = _scores(pdf[feat_cols].to_numpy(), contamination)
    return {int(i): float(v) for i, v in zip(pdf["row_id"].to_numpy(), s)}
