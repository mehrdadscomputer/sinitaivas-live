#!/bin/bash
set -euo pipefail
export PATH="/usr/bin:/bin:/usr/sbin:/sbin"

# Configuration
ROOT="/home/mehrdad/projects/bluesky_scrapper/sinitaivas-live"
LOG_FILE="$ROOT/main.log"
LINES_TO_KEEP=1000

# Truncate the log file if it exists
if [[ -f "$LOG_FILE" ]]; then
  tmp_file="$(mktemp)"
  tail -n "$LINES_TO_KEEP" "$LOG_FILE" > "$tmp_file"
  mv "$tmp_file" "$LOG_FILE"
  echo "Truncated $LOG_FILE to last $LINES_TO_KEEP lines."
else
  echo "Log file not found: $LOG_FILE"
  exit 1
fi
