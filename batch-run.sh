#!/usr/bin/env bash
# batch-run.sh — leveled-up runner for mailsieve.mjs
# Goals:
# - strict mode + safe tempfiles
# - robust normalization/deduping of domains.txt
# - resilient resume via processed.txt
# - optional parallelism (POLITE_CONCURRENCY), still host-paced by MAILSIEVE
# - atomic-ish appends; avoid repeated grep scans
# - keeps "name" output intact (MAILSIEVE handles it)
#
# Usage:
#   ./batch-run.sh
#   DOMAINS_FILE=domains.txt OUT=results.csv ./batch-run.sh
#   POLITE_CONCURRENCY=3 RATE_MS=900 QUIET=1 ./batch-run.sh
#
set -euo pipefail

# ---- Config ----
DOMAINS_FILE=${DOMAINS_FILE:-domains.txt}
OUT=${OUT:-results.csv}
PROCESSED=${PROCESSED:-processed.txt}
LOG_PATH=${LOG_PATH:-logs/evidence.jsonl}

# Script-level controls (MAILSIEVE has its own defaults)
QUIET_ENV=${QUIET_ENV:-1}
RATE_MS=${RATE_MS:-800}
HASH_EVIDENCE=${HASH_EVIDENCE:-1}
TIMEOUT_MS=${TIMEOUT_MS:-12000}
MAX_PAGES=${MAX_PAGES:-16}
RETRIES=${RETRIES:-3}
BACKOFF_MS=${BACKOFF_MS:-600}

# How many node processes to run concurrently. Keep low.
POLITE_CONCURRENCY=${POLITE_CONCURRENCY:-1}

# Ensure tools exist
command -v node >/dev/null 2>&1 || { echo "node is required" >&2; exit 1; }

# ---- Paths / setup ----
mkdir -p "$(dirname "$OUT")" "$(dirname "$PROCESSED")" "$(dirname "$LOG_PATH")"

# Header handling: only for CSV output. (If you output TSV/JSONL, you should use MAILSIEVE batch mode instead.)
CSV_HEADER='"company","owner","email"'
if [[ "$OUT" == *.csv ]]; then
  if [[ ! -f "$OUT" ]] || [[ ! -s "$OUT" ]]; then
    printf '%s\n' "$CSV_HEADER" > "$OUT"
  fi
else
  # For non-csv outputs, don't force a header. Just ensure the file exists.
  : > /dev/null
  [[ -f "$OUT" ]] || : > "$OUT"
fi

# Ensure state files exist
: > /dev/null
[[ -f "$PROCESSED" ]] || : > "$PROCESSED"
[[ -f "$LOG_PATH" ]] || : > "$LOG_PATH"

# ---- Temp workspace ----
TMPDIR=${TMPDIR:-/tmp}
RUN_ID="of_$$.$(date +%s)"
WORKDIR="$TMPDIR/$RUN_ID"
mkdir -p "$WORKDIR"
trap 'rm -rf "$WORKDIR"' EXIT

PROCESSED_SET="$WORKDIR/processed.set"
INPUT_NORM="$WORKDIR/domains.norm"
QUEUE="$WORKDIR/queue"
LOCK_OUT="$WORKDIR/out.lock"
LOCK_PROCESSED="$WORKDIR/processed.lock"

# ---- Helpers ----
normalize_domain() {
  # Reads stdin lines and prints normalized domains:
  # - strips comments after '#'
  # - trims whitespace
  # - removes scheme
  # - strips leading www.
  # - keeps only first token
  # - basic sanity: must contain a dot
  # NOTE: MAILSIEVE will do proper PSL normalization; this is just pre-filtering.
  sed 's/#.*$//' \
  | awk '{print $1}' \
  | sed -E 's/^[[:space:]]+|[[:space:]]+$//g' \
  | sed -E 's#^https?://##i' \
  | sed -E 's#^www\.##i' \
  | awk 'NF{print tolower($0)}' \
  | awk 'index($0,".")>0{print $0}'
}

# Atomic-ish append using a lock directory (portable enough for Linux/macOS)
with_lock() {
  local lockdir="$1"; shift
  local i=0
  while ! mkdir "$lockdir" 2>/dev/null; do
    i=$((i+1))
    # after ~10s, still continue but slow down
    if (( i > 200 )); then sleep 0.1; else sleep 0.05; fi
  done
  # shellcheck disable=SC2068
  "$@"
  rmdir "$lockdir" 2>/dev/null || true
}

append_line() {
  local file="$1"; shift
  local line="$1"
  printf '%s\n' "$line" >> "$file"
}

# ---- Build processed set ----
# Keep as a unique, normalized list.
normalize_domain < "$PROCESSED" | sort -u > "$PROCESSED_SET" || true

# ---- Normalize + dedupe input list, and remove already processed ----
normalize_domain < "$DOMAINS_FILE" | sort -u > "$INPUT_NORM"

# Filter out processed
comm -23 "$INPUT_NORM" "$PROCESSED_SET" > "$QUEUE" || true

TOTAL=$(wc -l < "$QUEUE" | tr -d ' ')
if [[ "$TOTAL" == "0" ]]; then
  echo "Nothing to do (all domains already processed)." >&2
  exit 0
fi

echo "Queued: $TOTAL domain(s). Concurrency: $POLITE_CONCURRENCY" >&2

# ---- Worker ----
run_one() {
  local dom="$1"

  # Run MAILSIEVE in single mode and capture exactly one CSV row (no headers)
  # Use a temp file to avoid partial writes on failure.
  local tmp_out="$WORKDIR/out.$(echo "$dom" | tr -c 'a-z0-9._-' '_').$$"

  # Environment passed to MAILSIEVE
  QUIET="$QUIET_ENV" \
  RATE_MS="$RATE_MS" \
  HASH_EVIDENCE="$HASH_EVIDENCE" \
  TIMEOUT_MS="$TIMEOUT_MS" \
  MAX_PAGES="$MAX_PAGES" \
  RETRIES="$RETRIES" \
  BACKOFF_MS="$BACKOFF_MS" \
  LOG_PATH="$LOG_PATH" \
  node mailsieve.mjs --noHeaders --domain "$dom" > "$tmp_out" 2>/dev/null \
    || { rm -f "$tmp_out"; return 0; }

  # If the script produced nothing (e.g., network blocked), do not mark processed.
  if [[ ! -s "$tmp_out" ]]; then
    rm -f "$tmp_out"
    return 0
  fi

  # Append result row to OUT under lock
  with_lock "$LOCK_OUT" bash -c 'cat "$1" >> "$2"' bash "$tmp_out" "$OUT"
  rm -f "$tmp_out"

  # Record processed domain (normalized as we store it)
  with_lock "$LOCK_PROCESSED" append_line "$PROCESSED" "$dom"
}

# ---- Execution strategy ----
# If POLITE_CONCURRENCY=1, do a simple loop.
# If >1, use xargs -P (available on GNU; on macOS/BSD, -P exists too).

if (( POLITE_CONCURRENCY <= 1 )); then
  while IFS= read -r dom; do
    [[ -z "$dom" ]] && continue
    run_one "$dom"
  done < "$QUEUE"
else
  # Export needed functions/vars for subshell execution
  export -f run_one with_lock append_line
  export WORKDIR OUT PROCESSED LOG_PATH QUIET_ENV RATE_MS HASH_EVIDENCE TIMEOUT_MS MAX_PAGES RETRIES BACKOFF_MS LOCK_OUT LOCK_PROCESSED

  # shellcheck disable=SC2016
  cat "$QUEUE" | xargs -I{} -P "$POLITE_CONCURRENCY" bash -c 'run_one "$@"' _ {}
fi

echo "Done." >&2
