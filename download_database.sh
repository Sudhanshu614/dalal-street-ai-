#!/bin/bash
set -euo pipefail
BUCKET=${BUCKET:-dalal-street-database-storage}
OBJECT=${OBJECT:-stock_market_new.db}
DEST=${DEST:-$HOME/data/stock_market_new.db}
TMP="$DEST.tmp"
mkdir -p "$(dirname "$DEST")"
if command -v gsutil >/dev/null 2>&1; then
  gsutil -m cp "gs://$BUCKET/$OBJECT" "$TMP"
elif command -v gcloud >/dev/null 2>&1; then
  gcloud storage cp "gs://$BUCKET/$OBJECT" "$TMP"
else
  echo "No gsutil/gcloud found"; exit 1
fi
mv -f "$TMP" "$DEST"
ls -lh "$DEST"
if [ "${FETCH_CFCA:-0}" = "1" ]; then
  mkdir -p "$HOME/data"
  if command -v gsutil >/dev/null 2>&1; then
    gsutil -m cp "gs://$BUCKET/CF-CA*.csv" "$HOME/data/" || true
  else
    gcloud storage cp "gs://$BUCKET/CF-CA*.csv" "$HOME/data/" || true
  fi
  ls -lh "$HOME"/data/CF-CA*.csv 2>/dev/null || true
fi