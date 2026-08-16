# Demo-Day Script — Partition-Aware Isolation Forest (≈ 20 minutes)

Two presenters (Arda + Batuhan). Timing cues in **[brackets]**. Lines are talking
points, not a word-for-word read — say them in your own words. Commands to run
live are in code blocks.

---

## 0. Title & hook — **[0:00–1:00]**
- "Our project is about detecting when a machine is starting to fail, from its
  sensor data, at scale — on a Spark cluster."
- The twist: distributing the detector can quietly *break* it. We found the cause,
  fixed it, and — importantly — proved with an ablation that our fix is what
  matters.

## 1. The problem — **[1:00–3:00]**
- Predictive maintenance: engines emit ~21 correlated sensors; failure shows up as
  slow *drift* over many cycles, not a single spike.
- Isolation Forest is a great, cheap anomaly detector, and people distribute it on
  Spark. But distributed iForest treats every row independently.
- When Spark splits a run-to-failure trajectory across workers, consecutive cycles
  land on different partitions. **The temporal order is lost.** The detector sees
  each cycle in isolation and misses the drift.
- One-line contribution: *preserve temporal locality across partition boundaries
  before building the trees.*

## 2. Related work & the gap — **[3:00–4:30]**
- Isolation Forest (Liu et al. 2008); distributed iForest on Spark (Togbe et al.
  2022; spark-iforest; LinkedIn's implementation).
- C-MAPSS is normally used for RUL regression with sequence models — which
  *confirms* temporal context carries the signal.
- Gap: nobody keeps the distributed iForest temporally coherent across partitions.

## 3. Dataset — **[4:30–6:00]**
- NASA C-MAPSS turbofan: 4 subsets, 709 engines, 160,359 cycles; 1 or 6 operating
  conditions, 1 or 2 fault modes.
- No native anomaly labels → we derive them: a cycle is "degraded" if within 30
  cycles of failure (RUL ≤ 30).

## 4. Method — **[6:00–10:00]**
Walk the five stages (point at the pipeline diagram / slide):
1. **Ingest** raw text → Parquet (Spark).
2. **Preprocess:** drop constant sensors; recover the 6 operating regimes with
   MLlib k-means; condition-wise standardization.
3. **Partition-aware windowing (the core idea):**
   - Add rolling mean, rolling std, and a W-cycle delta per sensor (W = 20) so
     drift is visible inside one feature vector.
   - Assign **whole engines** to partitions so a trajectory is never split — every
     window is computed over the correct neighbours.
4. **Detect:** a scikit-learn Isolation Forest *per Spark partition* via
   `applyInPandas` (MLlib has no iForest).
5. **Evaluate:** four configurations (next section).

## 5. Live demo — **[10:00–13:30]**
Have the dataset already downloaded and dependencies installed beforehand.

Show the cluster is real (optional, if Docker ready):
```bash
docker compose -f docker/docker-compose.yml up -d --scale spark-worker=2
# open http://localhost:8080  -> show master + 2 workers registered
```
Run the detection pipeline (or show a pre-run results file if time is tight):
```bash
DATA_DIR=./CMAPSSData python3 code/pipeline.py
```
- Point out the per-subset lines it prints (ref / naive / unaware / proposed AUC).
- Open `results/results.json` or the two figures to show the numbers are real.

> Tip: `pipeline.py` runs all four subsets in a couple of minutes. If the room is
> impatient, edit the subset list at the bottom of `main()` to `["FD001"]` for a
> ~30-second run, and say the full four-subset numbers are in `results.json`.

## 6. Results — **[13:30–17:30]**
Show Table III and the two bar charts.
- **Proposed (C4) has the best mean ROC-AUC (0.928) and F1 (0.651)**; biggest win
  on the hard six-condition FD002 (0.877 → 0.918).
- Be honest: on FD003/FD004 it's competitive but slightly below the instantaneous
  baselines — we report it straight.
- **The punchline — the ablation (C3):** take the *same* window features but
  compute them on partition-unaware splits → ROC-AUC collapses to 0.740, *worse
  than no windowing at all.* Since C3 and C4 differ only in partitioning, this
  proves partition-awareness is the cause of the gain, not the features.

## 7. Scalability & honesty — **[17:30–19:00]**
- Detection accuracy is environment-independent, so those numbers are final.
- Throughput needs real parallel workers: we provide the Docker Compose cluster and
  a sweep script; show `fig_scalability.png` if you've run it, otherwise state it's
  the throughput-vs-workers study and show the cluster UI.
- Limitations (say them plainly): per-partition score calibration; the FD003/FD004
  behaviour is an open question.

## 8. Conclusion & Q&A — **[19:00–20:00]**
- One sentence: "Keep trajectories intact across partitions, and a distributed
  Isolation Forest recovers the degradation signal it otherwise loses — and we
  proved partition-awareness is the reason."
- Invite questions.

---

### Likely questions — be ready
- *"Why per-partition sklearn instead of a native Spark iForest?"* MLlib has none;
  `applyInPandas` is the standard data-parallel pattern and is exactly where the
  partitioning effect lives.
- *"Why does it lose on FD003/FD004?"* Two fault modes + added feature
  dimensionality; it's in our limitations and is our next experiment.
- *"Is the ablation fair?"* Yes — C3 and C4 use identical features and the same
  detector; the *only* difference is whether trajectories stay intact.
- *"Are the scores comparable across partitions?"* Not perfectly — calibration is
  listed as future work.
