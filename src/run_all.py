"""CLI entry point: run every configuration on every subset and write results.json.
Usage (from project root):  python -m src.run_all   (or: python src/run_all.py)"""
import json, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.spark_utils import get_spark
from src.experiment import run_all
from src.config import RESULTS


def main():
    spark = get_spark()
    results = run_all(spark)
    with open(RESULTS / "results.json", "w") as f:
        json.dump(results, f, indent=2)
    for s, r in results.items():
        print(f"[{s}] AUC ref={r['C1_ref']['auc']:.3f} naive={r['C2_naive']['auc']:.3f} "
              f"unaware={r['C3_win_unaware']['auc']:.3f} proposed={r['C4_proposed']['auc']:.3f}")
    print("wrote", RESULTS / "results.json")
    spark.stop()


if __name__ == "__main__":
    main()
