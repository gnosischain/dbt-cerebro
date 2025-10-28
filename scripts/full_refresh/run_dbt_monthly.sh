#!/usr/bin/env bash
set -euo pipefail

# run_dbt_monthly.sh - Run dbt month-by-month with optional field batching
# Usage:
#   ./run_dbt_monthly.sh <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] [field:min:max:batch_size]
#   ./run_dbt_monthly.sh <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model> [field:min:max:batch_size]
#
# Examples:
#   ./run_dbt_monthly.sh my_model 2022-01-01 2022-12-01 1
#   ./run_dbt_monthly.sh my_model 2022-01-01 2022-12-01 1 validator_index:0:500000:50000

DEFAULT_MODEL="int_execution_transactions_by_project_daily"

is_month() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-01$ ]]
}

# Parse arguments
ARGS=()
for a in "$@"; do
  [[ -n "${a// /}" ]] && ARGS+=("$a")
done

if (( ${#ARGS[@]} < 2 )); then
  echo "Usage:"
  echo "  $0 <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] [field:min:max:batch_size]"
  echo "  $0 <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model> [field:min:max:batch_size]"
  exit 1
fi

MODEL=""
START_MONTH=""
END_MONTH=""
BATCH_SIZE="1"
FIELD_BATCH=""

# Detect pattern
if is_month "${ARGS[0]:-}" && is_month "${ARGS[1]:-}"; then
  # Form: START END [BATCH] [MODEL] [FIELD]
  START_MONTH="${ARGS[0]}"
  END_MONTH="${ARGS[1]}"
  
  if (( ${#ARGS[@]} >= 3 )) && [[ "${ARGS[2]}" =~ ^[0-9]+$ ]]; then
    BATCH_SIZE="${ARGS[2]}"
    MODEL="${ARGS[3]:-$DEFAULT_MODEL}"
    FIELD_BATCH="${ARGS[4]:-}"
  elif (( ${#ARGS[@]} >= 3 )) && [[ "${ARGS[2]}" =~ .*:.* ]]; then
    FIELD_BATCH="${ARGS[2]}"
    MODEL="${ARGS[3]:-$DEFAULT_MODEL}"
  else
    MODEL="${ARGS[2]:-$DEFAULT_MODEL}"
    FIELD_BATCH="${ARGS[3]:-}"
  fi
else
  # Form: MODEL START END [BATCH] [FIELD]
  MODEL="${ARGS[0]}"
  START_MONTH="${ARGS[1]:-}"
  END_MONTH="${ARGS[2]:-}"
  
  if (( ${#ARGS[@]} >= 4 )) && [[ "${ARGS[3]}" =~ ^[0-9]+$ ]]; then
    BATCH_SIZE="${ARGS[3]}"
    FIELD_BATCH="${ARGS[4]:-}"
  else
    FIELD_BATCH="${ARGS[3]:-}"
  fi
fi

# Validate
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

# Parse field batching if provided
BATCH_FIELD=""
FIELD_START=""
FIELD_END=""
FIELD_SIZE=""

if [[ -n "$FIELD_BATCH" ]] && [[ "$FIELD_BATCH" =~ .*:.* ]]; then
  IFS=':' read -r BATCH_FIELD FIELD_START FIELD_END FIELD_SIZE <<< "$FIELD_BATCH"
  
  # Validate field batch parameters
  if [[ -z "$BATCH_FIELD" ]] || [[ -z "$FIELD_START" ]] || [[ -z "$FIELD_END" ]] || [[ -z "$FIELD_SIZE" ]]; then
    echo "Error: Field batch format must be field_name:min:max:batch_size"
    echo "Got: $FIELD_BATCH"
    exit 1
  fi
  
  if ! [[ "$FIELD_START" =~ ^[0-9]+$ ]] || ! [[ "$FIELD_END" =~ ^[0-9]+$ ]] || ! [[ "$FIELD_SIZE" =~ ^[0-9]+$ ]]; then
    echo "Error: Field batch min, max, and size must be integers"
    exit 1
  fi
  
  echo "Field batching enabled:"
  echo "  Field: $BATCH_FIELD"
  echo "  Range: $FIELD_START to $FIELD_END"
  echo "  Batch size: $FIELD_SIZE"
fi

echo "Model:        $MODEL"
echo "Start month:  $START_MONTH"
echo "End month:    $END_MONTH"
echo "Batch size:   $BATCH_SIZE months"
echo

# Helper functions
to_int() {
  local y="${1:0:4}" m="${1:5:2}"
  echo "$((10#$y * 100 + 10#$m))"
}

add_months() {
  local date="$1" add="$2"
  local y="${date:0:4}" m="${date:5:2}"
  local yi=$((10#$y)) mi=$((10#$m))
  local total=$(( yi * 12 + (mi - 1) + add ))
  local ny=$(( total / 12 )) nm=$(( total % 12 + 1 ))
  printf "%04d-%02d-01\n" "$ny" "$nm"
}

min_month() {
  local a="$1" b="$2"
  if [[ "$(to_int "$a")" -le "$(to_int "$b")" ]]; then echo "$a"; else echo "$b"; fi
}

# Main processing loop
cur="$START_MONTH"
end="$END_MONTH"
first_batch=1
total_batches=0

while [[ "$(to_int "$cur")" -le "$(to_int "$end")" ]]; do
  batch_end_candidate="$(add_months "$cur" $((BATCH_SIZE - 1)))"
  batch_end="$(min_month "$batch_end_candidate" "$end")"
  
  if [[ -n "$BATCH_FIELD" ]]; then
    # Process with field batching
    field_current=$FIELD_START
    
    while [ $field_current -lt $FIELD_END ]; do
      field_batch_end=$((field_current + FIELD_SIZE))
      if [ $field_batch_end -gt $FIELD_END ]; then
        field_batch_end=$FIELD_END
      fi
      
      echo "==> ${MODEL}: ${cur} -> ${batch_end}, ${BATCH_FIELD}: ${field_current} -> ${field_batch_end}"
      
      # Build vars string with field batching
      vars="{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\""
      vars="${vars}, \"${BATCH_FIELD}_start\": ${field_current}"
      vars="${vars}, \"${BATCH_FIELD}_end\": ${field_batch_end}}"
      
      if (( first_batch )); then
        dbt run -s "$MODEL" --full-refresh --vars "$vars"
        first_batch=0
      else
        dbt run -s "$MODEL" --vars "$vars"
      fi
      
      total_batches=$((total_batches + 1))
      field_current=$field_batch_end
      
      # Small pause to prevent overwhelming the system
      sleep 1
    done
    
  else
    # Original behavior without field batching
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
    
    total_batches=$((total_batches + 1))
  fi
  
  cur="$(add_months "$batch_end" 1)"
done

echo "All batches completed for ${MODEL}. Total batches processed: ${total_batches}"