#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

# require a log file path (no default)
usage() {
  cat <<EOF >&2
Usage: $(basename "$0") LOG_FILE

  LOG_FILE   Path to the application log file (sinitaivas-live.log or main.log).
             (No default; must be provided as arg or via \$LOG_FILE)
EOF
  exit 1
}

# ensure positional or env var is set
if [[ -z "${1:-}" && -z "${LOG_FILE:-}" ]]; then
  echo "Error: missing log file path." >&2
  usage
fi

# assign either the arg or the env var
LOG_FILE="${1:-$LOG_FILE}"

service_name="sinitaivas-live.service"

# Get the timestamp of the last log entry
last_log_time=$(tail -n 1 "$LOG_FILE" | awk -F'|' '{gsub(/^ +| +$/, "", $2); print $2}')
echo "Last log time: $last_log_time"

# Convert the timestamp to a date format
last_log_epoch=$(date -d "$last_log_time" +%s)
current_epoch=$(date -u +%s)
time_diff=$(( (current_epoch - last_log_epoch) / 60 ))

# Check if the time difference is greater than 10 minutes
if [ "$time_diff" -gt 10 ]; then
    echo "Last log entry is older than 10 minutes. Restarting service..."
    sudo systemctl restart "$service_name"

else
    echo "Log file is updated within the last 10 minutes. No action needed."
fi