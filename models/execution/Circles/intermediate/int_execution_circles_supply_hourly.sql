{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(hour, version, token_address, token_id)',
        unique_key='(hour, version, token_address, token_id)',
        partition_by='toStartOfMonth(hour)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'supply']
    )
}}

WITH supply_changes AS (
    SELECT
        toStartOfHour(block_timestamp) AS hour,
        version,
        token_address,
        token_id,
        toInt256(amount_raw) AS supply_delta_raw
    FROM {{ ref('int_execution_circles_transfers') }}
    WHERE from_address = {{ circles_zero_address() }}
      {{ apply_monthly_incremental_filter('block_timestamp', 'hour', 'true', lookback_days=2) }}

    UNION ALL

    SELECT
        toStartOfHour(block_timestamp) AS hour,
        version,
        token_address,
        token_id,
        -toInt256(amount_raw) AS supply_delta_raw
    FROM {{ ref('int_execution_circles_transfers') }}
    WHERE to_address = {{ circles_zero_address() }}
      {{ apply_monthly_incremental_filter('block_timestamp', 'hour', 'true', lookback_days=2) }}
),
hourly_changes AS (
    SELECT
        hour,
        version,
        token_address,
        token_id,
        sum(supply_delta_raw) AS supply_delta_raw
    FROM supply_changes
    GROUP BY 1, 2, 3, 4
),
{% if is_incremental() %}
rebuild_start AS (
    SELECT min(hour) AS min_hour
    FROM hourly_changes
),
previous_totals AS (
    SELECT
        version,
        token_address,
        token_id,
        argMax(total_supply_raw, hour) AS previous_total_supply_raw
    FROM {{ this }}
    WHERE hour < (SELECT min_hour FROM rebuild_start)
    GROUP BY 1, 2, 3
),
{% endif %}
with_running_totals AS (
    SELECT
        h.hour,
        h.version,
        h.token_address,
        h.token_id,
        h.supply_delta_raw,
        sum(h.supply_delta_raw) OVER (
            PARTITION BY h.version, h.token_address, h.token_id
            ORDER BY h.hour
        )
        {% if is_incremental() %}
            + coalesce(p.previous_total_supply_raw, toInt256(0))
        {% endif %} AS total_supply_raw
    FROM hourly_changes h
    {% if is_incremental() %}
    LEFT JOIN previous_totals p
      ON h.version = p.version
     AND h.token_address = p.token_address
     AND h.token_id = p.token_id
    {% endif %}
)

SELECT
    hour,
    version,
    token_address,
    token_id,
    supply_delta_raw,
    total_supply_raw
FROM with_running_totals
