#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

# --- CONFIGURATION ---
EMAIL="mehrdadscomputer@gmail.com"
SUBJECT="❌ Bluesky gzip/disk issue on cPouta server"

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

STREAM_ROOT="${1:-$STREAM_ROOT}"
DATA_DIR="$STREAM_ROOT/firehose_stream"

# sanity check
if [[ ! -d "$DATA_DIR" ]]; then
  echo "Error: data directory not found: $DATA_DIR" \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 2
fi

echo "Scanning for uncompressed .ndjson files under: $DATA_DIR"

# compute hours to exclude (current, previous, and two hours ago)
exclude_hours=()
for offset in 0 1 2; do
  exclude_hours+=("$(date -u -d "-$offset hours" +'%Y-%m-%dT%H')")
done

# check disk space before doing work
used_pct=$(df -P "$DATA_DIR" | awk 'NR==2 {print $5}' | tr -d '%')
if (( used_pct > 60 )); then
  echo "❌ Low disk space on $(hostname): only $((100 - used_pct))% free." \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 1
fi

# find and gzip
find "$DATA_DIR" -type f -name '*.ndjson' | while read -r file; do
  skip=false
  for hour in "${exclude_hours[@]}"; do
    if [[ "$file" == *"/$hour.ndjson" ]]; then
      echo "⏭ Skipping (too recent): $file"
      skip=true
      break
    fi
  done

  if [[ "$skip" == false ]]; then
    echo "📦 Gzipping: $file"
    if ! gzip "$file"; then
      echo "❌ Gzip failed for $file on $(hostname)" \
        | mail -s "$SUBJECT" "$EMAIL"
    fi
  fi
done

echo "✔ Done."
