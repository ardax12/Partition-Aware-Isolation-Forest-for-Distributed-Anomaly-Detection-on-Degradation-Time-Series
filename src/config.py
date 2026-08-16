"""Central configuration: paths and experiment parameters."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = Path(os.environ.get("DATA_DIR", ROOT / "data" / "raw"))
PARQUET  = Path(os.environ.get("PARQUET_DIR", ROOT / "data" / "processed" / "parquet"))
RESULTS  = Path(os.environ.get("OUT_DIR", ROOT / "results"))
FIGURES  = ROOT / "figures"
for _d in (PARQUET, RESULTS, FIGURES):
    _d.mkdir(parents=True, exist_ok=True)

SEED    = 42          # global random seed (deterministic results)
W_LABEL = 30          # a cycle is anomalous if RUL <= W_LABEL
W_WIN   = 20          # temporal sliding-window length (cycles)
N_PART  = 4           # number of simulated worker partitions

SUBSETS  = ["FD001", "FD002", "FD003", "FD004"]
N_COND   = {"FD001": 1, "FD002": 6, "FD003": 1, "FD004": 6}   # operating conditions
SENSORS  = [f"s_{i}" for i in range(1, 22)]
SETTINGS = ["setting_1", "setting_2", "setting_3"]
COLS     = ["unit_nr", "time_cycles"] + SETTINGS + SENSORS
