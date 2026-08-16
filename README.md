# Partition-Aware Isolation Forest for Distributed Anomaly Detection

**BIL 501 — Big Data & Distributed Data Processing (Research Track), TOBB ETÜ**
Arda Günaydın (231101075) · Mustafa Batuhan Taş (241111024)

A distributed anomaly-detection pipeline on Apache Spark that preserves temporal
locality across partition boundaries (**partition-aware windowing**), so an
Isolation Forest can detect gradual sensor degradation that a naive distributed
implementation misses. Evaluated on the NASA C-MAPSS turbofan benchmark.

> **Every number here is measured and exactly reproducible.** The pipeline is
> deterministic (fixed seed, content-based partitioning, sorted partitions):
> re-running `04_detection_experiment.ipynb` reproduces `results/results.json`
> bit-for-bit on any machine.

---

## Folder map

```
notebooks/     run the project stage by stage (start here):
                 01_data_ingestion        raw text -> Parquet
                 02_preprocessing         RUL labels, conditions, normalization
                 03_windowing             partition-aware temporal features
                 04_detection_experiment  4 configs x 4 subsets -> results.json
                 05_results               tables + figures
                 06_scalability           cluster throughput -> figure
src/           the pipeline as small, tested modules the notebooks import:
                 config · spark_utils · ingest · preprocess · windowing
                 detector · evaluate · experiment · plots · run_all
paper/         IEEE paper (paper.pdf) + LaTeX source (self-contained)
presentation/  slides.pptx + demo_script.md (20-min run sheet)
docker/        Dockerfile + docker-compose.yml (multi-node Spark cluster)
scripts/       download_data + run_scalability_sweep  (.sh and .ps1)
results/       results.json (detection)  ·  scalability.csv (throughput)
figures/       fig_auc · fig_f1 · fig_scalability
data/          dataset goes in data/raw/ (fetched by script; not bundled)
```

The notebooks are thin — each calls the matching `src/` module — so the logic
lives in one tested place and the notebooks stay readable for the demo.

## Verified headline results

Mean over FD001–FD004 (full per-subset table in `results/results.json` and the paper):

| Config | ROC-AUC | F1 |
|---|---|---|
| **C4 proposed (partition-aware)** | **0.927** | **0.647** |
| C2 naive distributed | 0.920 | 0.637 |
| C1 single-node reference | 0.916 | 0.635 |
| C3 windowed, partition-unaware (ablation) | 0.753 | 0.338 |

C4 has the best mean ROC-AUC and F1 and wins on FD001, FD002, and FD004. The
ablation (C3) collapses far below the naive baseline — showing **partition-
awareness**, not the window features, is the source of the gain.

Scalability (measured on the Docker cluster, `results/scalability.csv`):
**4,173 -> 7,315 -> 9,654 -> 9,384 rows/s** for 1/2/4/8 workers (near-linear to 4,
then a plateau as CPU cores saturate).

---

## Run it

```bash
# 1. dependencies + dataset
pip install -r requirements.txt
bash scripts/download_data.sh            # Windows: powershell scripts\download_data.ps1

# 2. open the notebooks in order and run top to bottom:
#      01_data_ingestion -> 02_preprocessing -> 03_windowing
#      04_detection_experiment   (writes results/results.json)
#      05_results                (tables + figures)

# 3. scalability (needs Docker), then notebook 06 to view it:
bash scripts/run_scalability_sweep.sh    # Windows: powershell scripts\run_scalability_sweep.ps1
```

Prefer the command line? `python -m src.run_all` runs the whole detection
experiment and writes `results/results.json` (same code the notebooks call).

## Software stack
Apache Spark / PySpark 4.1.2, Python 3.12, OpenJDK 21, scikit-learn 1.8.0,
PyArrow 25.0.0. The Isolation Forest runs per Spark partition via `applyInPandas`;
Spark MLlib k-means recovers operating conditions.

## For the demo
`presentation/demo_script.md` has the 20-minute timing, live commands, and answers
to likely questions. Notebooks 05 and 06 are the ones to screen-share.

## Troubleshooting
- **`ModuleNotFoundError: sklearn` from a Spark worker** — handled: `spark_utils`
  points Spark workers at your interpreter; just install into the same venv.
- **Docker worker won't register / job stuck WAITING** — give Docker Desktop
  several CPUs (Settings -> Resources), then re-run the sweep.

Dataset: NASA C-MAPSS (Saxena et al., PHM 2008), public.
