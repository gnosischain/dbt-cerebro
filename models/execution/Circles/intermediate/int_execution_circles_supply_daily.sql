{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, version, token_address, token_id)',
        unique_key='(date, version, token_address, token_id)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'supply']
    )
}}

WITH supply_changes AS (
    SELECT
        toDate(block_timestamp) AS date,
        version,
        token_address,
        token_id,
        toInt256(amount_raw) AS supply_delta_raw
    FROM {{ ref('int_execution_circles_transfers') }}
    WHERE from_address = {{ circles_zero_address() }}
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true', lookback_days=2) }}

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        version,
        token_address,
        token_id,
        -toInt256(amount_raw) AS supply_delta_raw
    FROM {{ ref('int_execution_circles_transfers') }}
    WHERE to_address = {{ circles_zero_address() }}
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true', lookback_days=2) }}
),
daily_changes AS (
    SELECT
        date,
        version,
        token_address,
        token_id,
        sum(supply_delta_raw) AS supply_delta_raw
    FROM supply_changes
    GROUP BY 1, 2, 3, 4
),
{% if is_incremental() %}
rebuild_start AS (
    SELECT min(date) AS min_date
    FROM daily_changes
),
previous_totals AS (
    SELECT
        version,
        token_address,
        token_id,
        argMax(total_supply_raw, date) AS previous_total_supply_raw
    FROM {{ this }}
    WHERE date < (SELECT min_date FROM rebuild_start)
    GROUP BY 1, 2, 3
),
{% endif %}
with_running_totals AS (
    SELECT
        d.date,
        d.version,
        d.token_address,
        d.token_id,
        d.supply_delta_raw,
        sum(d.supply_delta_raw) OVER (
            PARTITION BY d.version, d.token_address, d.token_id
            ORDER BY d.date
        )
        {% if is_incremental() %}
            + coalesce(p.previous_total_supply_raw, toInt256(0))
        {% endif %} AS total_supply_raw
    FROM daily_changes d
    {% if is_incremental() %}
    LEFT JOIN previous_totals p
      ON d.version = p.version
     AND d.token_address = p.token_address
     AND d.token_id = p.token_id
    {% endif %}
)

SELECT
    date,
    version,
    token_address,
    token_id,
    supply_delta_raw,
    total_supply_raw
FROM with_running_totals
