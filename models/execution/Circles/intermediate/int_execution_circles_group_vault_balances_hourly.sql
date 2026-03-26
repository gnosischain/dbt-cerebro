{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(hour, group_address, token_id)',
        unique_key='(hour, group_address, token_id)',
        partition_by='toStartOfMonth(hour)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH hourly_deltas AS (
    SELECT
        toStartOfHour(block_timestamp) AS hour,
        group_address,
        token_id,
        sum(delta_raw) AS net_delta_raw
    FROM {{ ref('int_execution_circles_group_collateral_diffs') }}
    WHERE 1 = 1
    {{ apply_monthly_incremental_filter('block_timestamp', 'hour', 'true', lookback_days=2) }}
    GROUP BY 1, 2, 3
),
latest_vaults AS (
    SELECT
        lower(decoded_params['group']) AS group_address,
        argMax(lower(decoded_params['vault']), tuple(block_number, transaction_index, log_index)) AS vault_address
    FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
    WHERE event_name = 'CreateVault'
    GROUP BY 1
),
overall_max_hour AS (
    SELECT
        least(
            toStartOfHour(now()),
            coalesce((SELECT max(toStartOfHour(block_timestamp)) FROM {{ ref('int_execution_circles_group_collateral_diffs') }}), toStartOfHour(now()))
        ) AS max_hour
),
{% if is_incremental() %}
current_partition AS (
    SELECT max(hour) AS max_hour
    FROM {{ this }}
),
prev_balances AS (
    SELECT group_address, token_id, balance_raw
    FROM {{ this }}
    WHERE hour = (SELECT max_hour FROM current_partition)
),
keys AS (
    SELECT DISTINCT group_address, token_id
    FROM (
        SELECT group_address, token_id FROM prev_balances
        UNION ALL
        SELECT group_address, token_id FROM hourly_deltas
    )
),
calendar AS (
    SELECT
        k.group_address,
        k.token_id,
        cp.max_hour + offset * 3600 AS hour
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_hour o
    ARRAY JOIN range(dateDiff('hour', cp.max_hour, o.max_hour)) AS offset
),
{% else %}
calendar AS (
    SELECT
        group_address,
        token_id,
        min_hour + offset * 3600 AS hour
    FROM (
        SELECT
            group_address,
            token_id,
            min(hour) AS min_hour,
            dateDiff('hour', min(hour), any(o.max_hour)) AS num_hours
        FROM hourly_deltas
        CROSS JOIN overall_max_hour o
        GROUP BY 1, 2
    )
    ARRAY JOIN range(num_hours + 1) AS offset
),
{% endif %}
balances AS (
    SELECT
        c.hour,
        c.group_address,
        c.token_id,
        sum(coalesce(d.net_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.group_address, c.token_id
            ORDER BY c.hour
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %} AS balance_raw
    FROM calendar c
    LEFT JOIN hourly_deltas d
      ON c.hour = d.hour
     AND c.group_address = d.group_address
     AND c.token_id = d.token_id
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON c.group_address = p.group_address
     AND c.token_id = p.token_id
    {% endif %}
)

SELECT
    b.hour,
    b.group_address,
    lv.vault_address,
    b.token_id,
    b.balance_raw
FROM balances b
LEFT JOIN latest_vaults lv
    ON b.group_address = lv.group_address
WHERE b.balance_raw > 0
