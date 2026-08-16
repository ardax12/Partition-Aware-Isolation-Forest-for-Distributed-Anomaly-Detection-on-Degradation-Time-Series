#!/usr/bin/env bash
# Fetch the NASA C-MAPSS dataset into data/raw/ (public mirror).
set -e
DIR="${1:-data/raw}"; mkdir -p "$DIR"; cd "$DIR"
BASE="https://raw.githubusercontent.com/edwardzjl/CMAPSSData/master"
for s in FD001 FD002 FD003 FD004; do
  for p in train test RUL; do
    echo "downloading ${p}_${s}.txt"; curl -fsSL -o "${p}_${s}.txt" "${BASE}/${p}_${s}.txt"
  done
done
echo "done -> $DIR"
