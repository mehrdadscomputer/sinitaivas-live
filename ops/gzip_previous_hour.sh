#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

# --- CONFIGURATION ---
EMAIL="mehrdadscomputer@gmail.com,masoud.fatemi1990@gmail.com"
SUBJECT="❌ Bluesky gzip failure on cPouta server"

usage() {
  cat <<-EOF
Usage: $(basename "$0") STREAM_ROOT

  STREAM_ROOT  Root of your firehose_stream data collection.  
               (No default; must be provided as arg or via \$STREAM_ROOT)
EOF
  exit 1
}

# --- Ensure positional or env var is set ---
if [[ -z "${1:-}" && -z "${STREAM_ROOT:-}" ]]; then
  echo "Error: missing path to data." >&2
  usage
fi

STREAM_ROOT="${1:-$STREAM_ROOT}"
DATA_DIR="$STREAM_ROOT/firehose_stream"

# --- Sanity check ---
if [[ ! -d "$DATA_DIR" ]]; then
  echo "Error: data directory not found: $DATA_DIR" >&2 \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 2
fi

# --- Compute safe file to gzip: two hours ago UTC ---
date_part=$(date -u -d '2 hours ago' +'%Y-%m-%d')
hour_part=$(date -u -d '2 hours ago' +'%Y-%m-%dT%H')
file_to_gzip="$DATA_DIR/$date_part/$hour_part.ndjson"

echo "Looking for file to gzip: $file_to_gzip"

if [[ ! -f "$file_to_gzip" ]]; then
  echo "⚠ File not found: $file_to_gzip" \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 0
fi

# --- Check disk space (fail if <5% free) ---
used_pct=$(df -P "$DATA_DIR" | awk 'NR==2 {print $5}' | tr -d '%')
free_pct=$((100 - used_pct))

if (( free_pct < 40 )); then
  echo "❌ Low disk space on $(hostname): only ${free_pct}% free." \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 1
fi


# --- Try gzipping ---
if gzip "$file_to_gzip"; then
  echo "✔ Gzipped: $file_to_gzip"
else
  echo "❌ Gzip failed for $file_to_gzip on $(hostname)" \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 1
fi
