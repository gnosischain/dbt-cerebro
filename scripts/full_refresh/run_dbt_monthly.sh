#!/usr/bin/env bash
set -euo pipefail

# run_dbt_monthly.sh - Run dbt month-by-month with optional field batching
#
# Usage:
#   Numeric batching (e.g. validator_index):
#     ./run_dbt_monthly.sh [--incremental-only] <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] [field:min:max:batch_size]
#     ./run_dbt_monthly.sh [--incremental-only] <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model> [field:min:max:batch_size]
#
#   List batching (e.g. symbol include / exclude):
#     Include:  field:val1,val2,...
#       ./run_dbt_monthly.sh [--incremental-only] <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] [field:val1,val2,...]
#       ./run_dbt_monthly.sh [--incremental-only] <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model> [field:val1,val2,...]
#
#     Exclude:  !field:val1,val2,...
#       ./run_dbt_monthly.sh [--incremental-only] <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] [!field:val1,val2,...]
#       ./run_dbt_monthly.sh [--incremental-only] <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model> [!field:val1,val2,...]
#
# Examples:
#   ./run_dbt_monthly.sh my_model 2022-01-01 2022-12-01 1
#   ./run_dbt_monthly.sh my_model 2022-01-01 2022-12-01 1 validator_index:0:500000:50000
#   ./run_dbt_monthly.sh 2022-01-01 2022-12-01 1 my_model symbol:DAI,USDC,GNO
#   ./run_dbt_monthly.sh --incremental-only 2020-07-01 2025-12-01 1 int_execution_tokens_balances_daily symbol:DAI,USDC
#   ./run_dbt_monthly.sh 2020-07-01 2025-12-01 1 int_execution_tokens_balances_daily '!symbol:DAI,USDC'   # all except DAI,USDC
#

DEFAULT_MODEL="int_execution_transactions_by_project_daily"

is_month() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-01$ ]]
}

INCREMENTAL_ONLY=0

# Parse arguments (strip empty, extract flags)
ARGS=()
for a in "$@"; do
  if [[ "$a" == "--incremental-only" ]]; then
    INCREMENTAL_ONLY=1
  elif [[ -n "${a// /}" ]]; then
    ARGS+=("$a")
  fi
done

if (( ${#ARGS[@]} < 2 )); then
  echo "Usage:"
  echo "  $0 [--incremental-only] <model> <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] [field:min:max:batch_size | field:val1,val2,... | !field:val1,val2,...]"
  echo "  $0 [--incremental-only] <start YYYY-MM-01> <end YYYY-MM-01> [batch_size] <model> [field:min:max:batch_size | field:val1,val2,... | !field:val1,val2,...]"
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
FIELD_MODE=""       # "numeric" or "list"
FIELD_START=""
FIELD_END=""
FIELD_SIZE=""
FIELD_VALUES=()
FIELD_VALUES_RAW=""
EXCLUDE_LIST=0      # 0 = include list, 1 = exclude list

if [[ -n "$FIELD_BATCH" ]] && [[ "$FIELD_BATCH" =~ .*:.* ]]; then
  IFS=':' read -r BATCH_FIELD PARAM2 PARAM3 PARAM4 <<< "$FIELD_BATCH"
  
  if [[ -n "$PARAM3" && -n "$PARAM4" ]]; then
    #######################################################
    # Numeric batching: field:min:max:batch_size
    #######################################################
    FIELD_MODE="numeric"
    FIELD_START="$PARAM2"
    FIELD_END="$PARAM3"
    FIELD_SIZE="$PARAM4"
    
    # Validate numeric batch parameters
    if [[ -z "$BATCH_FIELD" ]] || [[ -z "$FIELD_START" ]] || [[ -z "$FIELD_END" ]] || [[ -z "$FIELD_SIZE" ]]; then
      echo "Error: Field batch format must be field_name:min:max:batch_size"
      echo "Got: $FIELD_BATCH"
      exit 1
    fi
    
    if ! [[ "$FIELD_START" =~ ^[0-9]+$ ]] || ! [[ "$FIELD_END" =~ ^[0-9]+$ ]] || ! [[ "$FIELD_SIZE" =~ ^[0-9]+$ ]]; then
      echo "Error: Field batch min, max, and size must be integers"
      exit 1
    fi
    
    echo "Field batching (numeric) enabled:"
    echo "  Field: $BATCH_FIELD"
    echo "  Range: $FIELD_START to $FIELD_END"
    echo "  Batch size: $FIELD_SIZE"
  else
    #######################################################
    # List batching: field:val1,val2,... or !field:val1,val2,...
    #######################################################
    FIELD_MODE="list"
    if [[ -z "$BATCH_FIELD" ]] || [[ -z "$PARAM2" ]]; then
      echo "Error: Field list format must be field_name:val1,val2,... or !field_name:val1,val2,..."
      echo "Got: $FIELD_BATCH"
      exit 1
    fi

    EXCLUDE_LIST=0
    # If field starts with "!", treat it as an exclude list: !symbol:DAI,USDC,...
    if [[ "$BATCH_FIELD" == \!* ]]; then
      EXCLUDE_LIST=1
      BATCH_FIELD="${BATCH_FIELD:1}"   # strip leading "!"
    fi

    FIELD_VALUES_RAW="$PARAM2"
    IFS=',' read -r -a FIELD_VALUES <<< "$PARAM2"

    if (( EXCLUDE_LIST )); then
      echo "Field batching (list EXCLUDE) enabled:"
      echo "  Field: $BATCH_FIELD"
      echo "  Excluding (${#FIELD_VALUES[@]}): ${FIELD_VALUES[*]}"
    else
      echo "Field batching (list INCLUDE) enabled:"
      echo "  Field: $BATCH_FIELD"
      echo "  Values (${#FIELD_VALUES[@]}): ${FIELD_VALUES[*]}"
    fi
  fi
fi

echo "Model:              $MODEL"
echo "Start month:        $START_MONTH"
echo "End month:          $END_MONTH"
echo "Batch size:         $BATCH_SIZE months"
echo "Incremental only:   $INCREMENTAL_ONLY"
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
    if [[ "$FIELD_MODE" == "numeric" ]]; then
      #################################################
      # Numeric field batching (existing behaviour)
      #################################################
      field_current=$FIELD_START
      
      while [ $field_current -lt $FIELD_END ]; do
        field_batch_end=$((field_current + FIELD_SIZE))
        if [ $field_batch_end -gt $FIELD_END ]; then
          field_batch_end=$FIELD_END
        fi
        
        echo "==> ${MODEL}: ${cur} -> ${batch_end}, ${BATCH_FIELD}: ${field_current} -> ${field_batch_end}"
        
        # Build vars string with numeric field batching
        vars="{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\""
        vars="${vars}, \"${BATCH_FIELD}_start\": ${field_current}"
        vars="${vars}, \"${BATCH_FIELD}_end\": ${field_batch_end}}"
        
        if (( first_batch )) && (( INCREMENTAL_ONLY == 0 )); then
          dbt run -s "$MODEL" --full-refresh --vars "$vars"
          first_batch=0
        else
          dbt run -s "$MODEL" --vars "$vars"
          first_batch=0
        fi
        
        total_batches=$((total_batches + 1))
        field_current=$field_batch_end
        
        # Small pause to prevent overwhelming the system
        sleep 1
      done
    else
      #################################################
      # List field batching:
      #   INCLUDE: one run per value (symbol:DAI,USDC)
      #   EXCLUDE: one run per month (!symbol:DAI,USDC)
      #################################################
      if (( EXCLUDE_LIST )); then
        # Exclude mode: single run per month with *_exclude var
        echo "==> ${MODEL}: ${cur} -> ${batch_end}, excluding ${BATCH_FIELD}: ${FIELD_VALUES_RAW}"
        
        vars="{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\", \"${BATCH_FIELD}_exclude\": \"${FIELD_VALUES_RAW}\"}"
        
        if (( first_batch )) && (( INCREMENTAL_ONLY == 0 )); then
          dbt run -s "$MODEL" --full-refresh --vars "$vars"
          first_batch=0
        else
          dbt run -s "$MODEL" --vars "$vars"
          first_batch=0
        fi
        
        total_batches=$((total_batches + 1))
        sleep 1
      else
        # Include mode: one run per value
        for field_value in "${FIELD_VALUES[@]}"; do
          echo "==> ${MODEL}: ${cur} -> ${batch_end}, ${BATCH_FIELD}: ${field_value}"
          
          # Build vars for this value (e.g. symbol)
          vars="{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\", \"${BATCH_FIELD}\": \"${field_value}\"}"
          
          if (( first_batch )) && (( INCREMENTAL_ONLY == 0 )); then
            dbt run -s "$MODEL" --full-refresh --vars "$vars"
            first_batch=0
          else
            dbt run -s "$MODEL" --vars "$vars"
            first_batch=0
          fi
          
          total_batches=$((total_batches + 1))
          sleep 1
        done
      fi
    fi
  else
    ###################################################
    # Original behavior without any field batching
    ###################################################
    echo "==> ${MODEL}: ${cur} -> ${batch_end}"
    
    if (( first_batch )) && (( INCREMENTAL_ONLY == 0 )); then
      dbt run -s "$MODEL" \
        --full-refresh \
        --vars "{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\"}"
      first_batch=0
    else
      dbt run -s "$MODEL" \
        --vars "{\"start_month\": \"${cur}\", \"end_month\": \"${batch_end}\"}"
      first_batch=0
    fi
    
    total_batches=$((total_batches + 1))
  fi
  
  cur="$(add_months "$batch_end" 1)"
done

echo "All batches completed for ${MODEL}. Total batches processed: ${total_batches}"
