#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

usage() {
  cat <<-EOF
Usage: $(basename "$0") STREAM_ROOT

  STREAM_ROOT  Root of your firehose_stream data collection.  
               (No default; must be provided as arg or via \$STREAM_ROOT)
EOF
  exit 1
}

# ensure positional or env var is set
if [[ -z "${1:-}" && -z "${STREAM_ROOT:-}" ]]; then
  echo "Error: missing path to data." >&2
  usage
fi

# assign either the arg or the env var
STREAM_ROOT="${1:-$STREAM_ROOT}"
DATA_DIR="$STREAM_ROOT/firehose_stream"

# sanity check
if [[ ! -d "$DATA_DIR" ]]; then
  echo "Error: data directory not found: $DATA_DIR" >&2
  exit 2
fi

# compute hour that is safe to gzip: two hours ago UTC
date_part=$(date -u -d '2 hours ago' +'%Y-%m-%d')
hour_part=$(date -u -d '2 hours ago' +'%Y-%m-%dT%H')

file_to_gzip="$DATA_DIR/$date_part/$hour_part.ndjson"

echo "Looking for file to gzip: $file_to_gzip"

if [[ -f "$file_to_gzip" ]]; then
  gzip "$file_to_gzip"
  echo "✔ Gzipped: $file_to_gzip"
else
  echo "⚠ File not found, skipping: $file_to_gzip"
fi