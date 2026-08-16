# Multi-node scalability sweep (Windows). Run from project root.
$ErrorActionPreference = "Stop"
function dc { docker compose -f docker/docker-compose.yml @args }
if (-not (Test-Path ".\data\raw")) { Write-Error "Dataset missing -> powershell scripts\download_data.ps1" }
dc build
New-Item -ItemType Directory -Force -Path results | Out-Null
Remove-Item results\scalability.csv -ErrorAction SilentlyContinue
foreach ($n in 1,2,4,8) {
  Write-Host "=== $n worker(s) ===" -ForegroundColor Cyan
  dc up -d --scale spark-worker=$n
  Start-Sleep -Seconds 20
  dc exec -T -e WORKERS=$n -e SPARK_MASTER=spark://spark-master:7077 `
     -e DATA_DIR=/work/data/raw -e OUT_DIR=/work/results `
     spark-master spark-submit --master spark://spark-master:7077 /work/src/run_scalability.py
}
dc down
python src\make_figures.py
Write-Host "Done -> results\scalability.csv, figures\fig_scalability.png" -ForegroundColor Green
