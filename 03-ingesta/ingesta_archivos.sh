#!/usr/bin/env bash
set -euo pipefail
: "${BUCKET:?Falta BUCKET}"
DIR="${DIR:-$HOME/datos/dane/nacimientos}"

for f in "$DIR"/nacimientos_*.csv; do
  y=$(basename "$f" .csv | sed 's/.*_//')
  aws s3 sync "$DIR" "s3://$BUCKET/raw/archivos/nacimientos/anio=$y/" \
    --exclude "*" --include "nacimientos_$y.csv" --only-show-errors
done