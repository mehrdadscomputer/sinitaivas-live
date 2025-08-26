#!/usr/bin/env bash
set -euo pipefail
# --- CONFIGURATION ---
BASE_DIR="/home/mehrdad/projects/bluesky_scrapper/sinitaivas-live/firehose_stream"                  # root path where date dirs live
EMAIL="mehrdadscomputer@gmail.com masoud.fatemi1990@gmail.com"            # recipient email
SUBJECT="Bluesky Python script is not running anymore on cPouta server"      # email subject

# --- Current date and hour ---
DATE=$(date -d '-1 hours' +%Y-%m-%d)            # e.g. 2025-08-22
HOUR=$(date -d '-1 hours' +%H)                  # e.g. 13
TARGET_DIR="${BASE_DIR}/${DATE}"
TARGET_FILE="${TARGET_DIR}/${DATE}T${HOUR}.ndjson"

# --- Check directory ---
if [[ ! -d "$TARGET_DIR" ]]; then
  echo "Directory $TARGET_DIR does not exist." \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 0
fi

# --- Check file ---
if [[ ! -f "$TARGET_FILE" ]]; then
  echo "Expected file missing: $TARGET_FILE" \
    | mail -s "$SUBJECT" "$EMAIL"
  exit 0
fi

# Optional success log (useful when run via cron)
echo "$(date): File $TARGET_FILE exists."
