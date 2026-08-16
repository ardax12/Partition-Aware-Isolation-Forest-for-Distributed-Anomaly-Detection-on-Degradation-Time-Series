#!/usr/bin/env bash
# Multi-node scalability sweep -> results/scalability.csv, figures/fig_scalability.png
set -e; cd "$(dirname "$0")/.."
C="docker compose -f docker/docker-compose.yml"
[ -d data/raw ] || { echo "Dataset missing -> bash scripts/download_data.sh"; exit 1; }
$C build
rm -f results/scalability.csv
for n in 1 2 4 8; do
  echo "=== $n worker(s) ==="
  $C up -d --scale spark-worker=$n
  sleep 20
  $C exec -T -e WORKERS="$n" -e SPARK_MASTER=spark://spark-master:7077 \
     -e DATA_DIR=/work/data/raw -e OUT_DIR=/work/results \
     spark-master spark-submit --master spark://spark-master:7077 /work/src/run_scalability.py
done
$C down
python src/make_figures.py
echo "Done -> results/scalability.csv, figures/fig_scalability.png"
