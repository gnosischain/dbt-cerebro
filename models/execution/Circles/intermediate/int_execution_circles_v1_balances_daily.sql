{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, token_address, account)',
        unique_key='(date, token_address, token_id, account)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'circles_v1', 'balances']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
WITH deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        account,
        token_id,
        token_address,
        sum(delta_raw) AS net_delta_raw
    FROM {{ ref('int_execution_circles_v1_balance_diffs') }}
    WHERE toDate(block_timestamp) < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true', lookback_days=2) }}
      {% endif %}
    GROUP BY 1, 2, 3, 4
),
{% if is_incremental() %}
prev_balances AS (
    SELECT
        account,
        token_id,
        token_address,
        argMax(balance_raw, date) AS balance_raw
    FROM {{ this }}
    GROUP BY 1, 2, 3
),
{% endif %}
with_running_totals AS (
    SELECT
        d.date,
        d.account,
        d.token_id,
        d.token_address,
        d.net_delta_raw,
        sum(d.net_delta_raw) OVER (
            PARTITION BY d.account, d.token_id, d.token_address
            ORDER BY d.date
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %} AS balance_raw
    FROM deltas d
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON d.account = p.account
     AND d.token_id = p.token_id
     AND d.token_address = p.token_address
    {% endif %}
)

SELECT
    date,
    account,
    token_id,
    token_address,
    balance_raw
FROM with_running_totals
WHERE balance_raw != 0
