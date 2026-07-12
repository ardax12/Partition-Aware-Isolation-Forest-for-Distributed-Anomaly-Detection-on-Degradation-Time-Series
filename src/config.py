from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

RAW_DATA = ROOT_DIR / "data/raw"
PROCESSED_DATA = ROOT_DIR / "data/processed"
FIGURES = ROOT_DIR / "figures"


TRAIN_FILE = RAW_DATA_DIR / "train_FD001.txt"
TEST_FILE = RAW_DATA_DIR / "test_FD001.txt"
RUL_FILE = RAW_DATA_DIR / "RUL_FD001.txt"

CLEAN_DATA_FILE = PROCESSED_DATA_DIR / "train_clean.parquet"
RUL_DATA_FILE = PROCESSED_DATA_DIR / "train_rul.parquet"

APP_NAME = "NASA Predictive Maintenance"
MASTER = "local[*]"