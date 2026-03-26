{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, group_address, token_id)',
        unique_key='(date, group_address, token_id)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        group_address,
        token_id,
        sum(delta_raw) AS net_delta_raw
    FROM {{ ref('int_execution_circles_group_collateral_diffs') }}
    WHERE 1 = 1
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true', lookback_days=2) }}
    GROUP BY 1, 2, 3
),
overall_max_date AS (
    SELECT
        least(
            today(),
            coalesce((SELECT max(toDate(block_timestamp)) FROM {{ ref('int_execution_circles_group_collateral_diffs') }}), today())
        ) AS max_date
),
{% if is_incremental() %}
current_partition AS (
    SELECT max(date) AS max_date
    FROM {{ this }}
),
prev_balances AS (
    SELECT group_address, token_id, balance_raw
    FROM {{ this }}
    WHERE date = (SELECT max_date FROM current_partition)
),
keys AS (
    SELECT DISTINCT group_address, token_id
    FROM (
        SELECT group_address, token_id FROM prev_balances
        UNION ALL
        SELECT group_address, token_id FROM deltas
    )
),
calendar AS (
    SELECT
        k.group_address,
        k.token_id,
        addDays(cp.max_date + 1, offset) AS date
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(dateDiff('day', cp.max_date, o.max_date)) AS offset
),
{% else %}
calendar AS (
    SELECT
        group_address,
        token_id,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            group_address,
            token_id,
            min(date) AS min_date,
            dateDiff('day', min(date), any(o.max_date)) AS num_days
        FROM deltas
        CROSS JOIN overall_max_date o
        GROUP BY 1, 2
    )
    ARRAY JOIN range(num_days + 1) AS offset
),
{% endif %}
balances AS (
    SELECT
        c.date,
        c.group_address,
        c.token_id,
        sum(coalesce(d.net_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.group_address, c.token_id
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %} AS balance_raw
    FROM calendar c
    LEFT JOIN deltas d
      ON c.date = d.date
     AND c.group_address = d.group_address
     AND c.token_id = d.token_id
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON c.group_address = p.group_address
     AND c.token_id = p.token_id
    {% endif %}
)

SELECT
    date,
    group_address,
    token_id,
    balance_raw
FROM balances
WHERE balance_raw > 0
