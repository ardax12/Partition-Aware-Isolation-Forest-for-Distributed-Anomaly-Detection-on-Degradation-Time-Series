"""Stage 5b - metrics. ROC-AUC and average precision from the anomaly scores,
plus F1 at a prevalence-matched threshold (flag exactly as many cycles as there
are true anomalies, where precision = recall = F1)."""
import numpy as np
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_fscore_support


def evaluate(scores_by_id, labels_by_id):
    ids = sorted(scores_by_id)                      # sorted -> deterministic
    y = np.array([labels_by_id[i] for i in ids])
    s = np.array([scores_by_id[i] for i in ids])
    auc = roc_auc_score(y, s)
    ap = average_precision_score(y, s)
    k = int(y.sum())
    thr = np.sort(s)[::-1][k - 1] if k > 0 else s.max() + 1
    pred = (s >= thr).astype(int)
    p, r, f, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    return dict(auc=float(auc), ap=float(ap), precision=float(p), recall=float(r),
                f1=float(f), n=int(len(y)), anomaly_rate=float(y.mean()))
