#!/bin/bash
set -euo pipefail
BUCKET=${BUCKET:-dalal-street-database-storage}
OBJECT=${OBJECT:-stock_market_new.db}
# Updated destination to match the backend service's expected path
DEST=${DEST:-/home/sudhanshubawane_work/dalal-street-ai-/App/database/stock_market_new.db}
TMP="$DEST.tmp"
mkdir -p "$(dirname "$DEST")"

if command -v gsutil >/dev/null 2>&1; then
  gsutil -m cp "gs://$BUCKET/$OBJECT" "$TMP"
elif command -v gcloud >/dev/null 2>&1; then
  gcloud storage cp "gs://$BUCKET/$OBJECT" "$TMP"
else
  echo "No gsutil/gcloud found"; exit 1
fi

mv "$TMP" "$DEST"
echo "Database downloaded to $DEST"
