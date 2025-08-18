#!/bin/bash
set -euo pipefail
export PATH="/usr/bin:/bin:/usr/sbin:/sbin"

ROOT="/home/mehrdad/projects/bluesky_scrapper/sinitaivas-live/firehose_stream"
KEEP_DAYS=2   # keep last N full days
DRY_RUN=0

cutoff="$(date -u -d "$KEEP_DAYS days ago" +%Y-%m-%d)"

while IFS= read -r daydir; do
  day="$(basename "$daydir")"
  [[ "$day" > "$cutoff" ]] && continue
  if (( DRY_RUN )); then
    echo "WOULD REMOVE: $daydir"
  else
    rm -rf -- "$daydir"
    echo "REMOVED:      $daydir"
  fi
done < <(find "$ROOT" -mindepth 1 -maxdepth 1 -type d -printf '%p\n' | sort)
