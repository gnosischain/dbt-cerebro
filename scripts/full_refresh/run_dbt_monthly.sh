#!/usr/bin/env bash
set -euo pipefail

# This script runs dbt month-by-month in batches.
# It accepts EITHER of these forms:
#   ./run_dbt_monthly.sh <model> 2022-08-01 2022-12-01 [batch_size]
#   ./run_dbt_monthly.sh 2022-08-01 2022-12-01 [batch_size] <model>
#
# Default model if omitted at the end: int_execution_transactions_by_project_daily
# Dates MUST be exactly YYYY-MM-01 (first of month), no newlines.

DEFAULT_MODEL="int_execution_transactions_by_project_daily"

is_month() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-01$ ]]
}

# ---- parse args (tolerant of order) ----
MODEL=""
START_MONTH=""
END_MONTH=""
BATCH_SIZE="1"

# collect non-empty args
ARGS=()
for a in "$@"; do
  [[ -n "${a// /}" ]] && ARGS+=("$a")
done

if (( ${#ARGS[@]} < 2 )); then
  echo "Usage:"
  echo "  $0 <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size]"
  echo "  $0 <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model>"
  exit 1
fi

# Try to detect pattern
if is_month "${ARGS[0]:-}" && is_month "${ARGS[1]:-}"; then
  # form: START END [BATCH] [MODEL]
  START_MONTH="${ARGS[0]}"
  END_MONTH="${ARGS[1]}"
  if (( ${#ARGS[@]} >= 3 )) && [[ "${ARGS[2]}" =~ ^[0-9]+$ ]]; then
    BATCH_SIZE="${ARGS[2]}"
    MODEL="${ARGS[3]:-$DEFAULT_MODEL}"
  else
    MODEL="${ARGS[2]:-$DEFAULT_MODEL}"
  fi
else
  # form: MODEL START END [BATCH]
  MODEL="${ARGS[0]}"
  START_MONTH="${ARGS[1]:-}"
  END_MONTH="${ARGS[2]:-}"
  if (( ${#ARGS[@]} >= 4 )) && [[ "${ARGS[3]}" =~ ^[0-9]+$ ]]; then
    BATCH_SIZE="${ARGS[3]}"
  fi
fi

# ---- validate ----
if ! is_month "$START_MONTH" || ! is_month "$END_MONTH"; then
  echo "Error: START/END must be in YYYY-MM-01 format."
  echo "Got: START='$START_MONTH' END='$END_MONTH'"
  exit 1
fi
if ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] || (( BATCH_SIZE < 1 )); then
  echo "Error: batch_size must be a positive integer. Got '$BATCH_SIZE'"
  exit 1
fi
if [[ -z "$MODEL" ]]; then MODEL="$DEFAULT_MODEL"; fi

echo "Model:        $MODEL"
echo "Start month:  $START_MONTH"
echo "End month:    $END_MONTH"
echo "Batch size:   $BATCH_SIZE"
echo

# ---- helpers ----
to_int() { # "YYYY-MM-01" -> YYYYMM integer
  local y="${1:0:4}" m="${1:5:2}"
  echo "$((10#$y * 100 + 10#$m))"
}

add_months() { # add N months to YYYY-MM-01
  local date="$1" add="$2"
  local y="${date:0:4}" m="${date:5:2}"
  local yi=$((10#$y)) mi=$((10#$m))
  local total=$(( yi * 12 + (mi - 1) + add ))
  local ny=$(( total / 12 )) nm=$(( total % 12 + 1 ))
  printf "%04d-%02d-01\n" "$ny" "$nm"
}

min_month() { # min by month
  local a="$1" b="$2"
  if [[ "$(to_int "$a")" -le "$(to_int "$b")" ]]; then echo "$a"; else echo "$b"; fi
}

# ---- loop ----
cur="$START_MONTH"
end="$END_MONTH"
first_batch=1

while [[ "$(to_int "$cur")" -le "$(to_int "$end")" ]]; do
  batch_end_candidate="$(add_months "$cur" $((BATCH_SIZE - 1)))"
  batch_end="$(min_month "$batch_end_candidate" "$end")"

  echo "==> ${MODEL}: ${cur} -> ${batch_end}"

  if (( first_batch )); then
    dbt run -s "$MODEL" \
      --full-refresh \
      --vars "{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\"}"
    first_batch=0
  else
    dbt run -s "$MODEL" \
      --vars "{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\"}"
  fi

  cur="$(add_months "$batch_end" 1)"
done

echo "All batches completed for ${MODEL}."
