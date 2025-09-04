#!/usr/bin/env bash
set -euo pipefail

# --- CONFIGURATION ---
EMAIL="mehrdadscomputer@gmail.com,masoud.fatemi1990@gmail.com"
SUBJECT="⚠️ Low Disk Space on cPouta Server"
THRESHOLD_GB=40
MOUNT_POINT="/home"
LOG_FILE="/home/mehrdad/projects/bluesky_scrapper/sinitaivas-live/logs/disk_space_check.log"

# --- CHECK AVAILABLE DISK SPACE ---
available_gb=$(df -BG "$MOUNT_POINT" | awk 'NR==2 {gsub("G","",$4); print $4}')

timestamp=$(date "+%Y-%m-%d %H:%M:%S")

# --- SEND EMAIL IF SPACE BELOW THRESHOLD ---
if (( available_gb < THRESHOLD_GB )); then
  HOSTNAME=$(hostname)
  MESSAGE="[$timestamp] WARNING: Only ${available_gb} GB of disk space left on ${MOUNT_POINT} at ${HOSTNAME}."
  echo "$MESSAGE" | tee -a "$LOG_FILE" | mail -s "$SUBJECT" "$EMAIL"
  exit 1
else
  echo "[$timestamp] OK: Disk space is sufficient (${available_gb} GB available on ${MOUNT_POINT})." >> "$LOG_FILE"
fi
