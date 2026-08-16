# Data

The NASA C-MAPSS turbofan degradation dataset is **not bundled** (public, ~40 MB).

Fetch it into `data/raw/`:

```bash
bash scripts/download_data.sh          # macOS/Linux
powershell scripts\download_data.ps1   # Windows
```

- `data/raw/`        raw C-MAPSS text files (train/test/RUL x FD001-FD004)
- `data/processed/`  Parquet written by the pipeline (created automatically)

Source: NASA Prognostics Data Repository (Saxena et al., PHM 2008).
