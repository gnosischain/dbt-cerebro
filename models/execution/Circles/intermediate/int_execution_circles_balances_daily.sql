{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, version, token_address, account)',
        unique_key='(date, version, token_address, token_id, account)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'balances']
    )
}}

WITH deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        version,
        account,
        token_id,
        token_address,
        sum(delta_raw) AS net_delta_raw,
        max(toUInt64(toUnixTimestamp(block_timestamp))) AS last_activity_ts_for_day
    FROM {{ ref('int_execution_circles_balance_diffs') }}
    WHERE account != {{ circles_zero_address() }}
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true', lookback_days=2) }}
    GROUP BY 1, 2, 3, 4, 5
),
overall_max_date AS (
    SELECT
        least(
            today(),
            coalesce((SELECT max(toDate(block_timestamp)) FROM {{ ref('int_execution_circles_balance_diffs') }}), today())
        ) AS max_date
),
{% if is_incremental() %}
current_partition AS (
    SELECT max(date) AS max_date
    FROM {{ this }}
),
prev_balances AS (
    SELECT
        version,
        account,
        token_id,
        token_address,
        balance_raw,
        last_activity_ts
    FROM {{ this }}
    WHERE date = (SELECT max_date FROM current_partition)
),
keys AS (
    SELECT DISTINCT version, account, token_id, token_address
    FROM (
        SELECT version, account, token_id, token_address FROM prev_balances
        UNION ALL
        SELECT version, account, token_id, token_address FROM deltas
    )
),
calendar AS (
    SELECT
        k.version,
        k.account,
        k.token_id,
        k.token_address,
        addDays(cp.max_date + 1, offset) AS date
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(dateDiff('day', cp.max_date, o.max_date)) AS offset
),
{% else %}
calendar AS (
    SELECT
        version,
        account,
        token_id,
        token_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            version,
            account,
            token_id,
            token_address,
            min(date) AS min_date,
            dateDiff('day', min(date), any(o.max_date)) AS num_days
        FROM deltas
        CROSS JOIN overall_max_date o
        GROUP BY 1, 2, 3, 4
    )
    ARRAY JOIN range(num_days + 1) AS offset
),
{% endif %}
balances AS (
    SELECT
        c.date,
        c.version,
        c.account,
        c.token_id,
        c.token_address,
        sum(coalesce(d.net_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.version, c.account, c.token_id, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %} AS balance_raw,
        toUInt64(
            greatest(
                max(coalesce(d.last_activity_ts_for_day, toUInt64(0))) OVER (
                    PARTITION BY c.version, c.account, c.token_id, c.token_address
                    ORDER BY c.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                {% if is_incremental() %}
                    coalesce(p.last_activity_ts, toUInt64(0))
                {% else %}
                    toUInt64(0)
                {% endif %}
            )
        ) AS last_activity_ts
    FROM calendar c
    LEFT JOIN deltas d
      ON c.date = d.date
     AND c.version = d.version
     AND c.account = d.account
     AND c.token_id = d.token_id
     AND c.token_address = d.token_address
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON c.version = p.version
     AND c.account = p.account
     AND c.token_id = p.token_id
     AND c.token_address = p.token_address
    {% endif %}
),
snapshots AS (
    SELECT
        date,
        version,
        account,
        token_id,
        token_address,
        balance_raw,
        last_activity_ts,
        toUInt64(
            if(
                date = today(),
                {{ circles_chain_now_ts() }},
                toUnixTimestamp(addDays(toDateTime(date), 1)) - 1
            )
        ) AS snapshot_ts
    FROM balances
    WHERE balance_raw != 0
)

SELECT
    date,
    version,
    account,
    token_id,
    token_address,
    balance_raw,
    last_activity_ts,
    snapshot_ts,
    if(
        version = 2,
        toUInt256(
            multiplyDecimal(
                toDecimal256(greatest(balance_raw, toInt256(0)), 0),
                {{ circles_demurrage_factor('last_activity_ts', 'snapshot_ts') }},
                0
            )
        ),
        toUInt256(greatest(balance_raw, toInt256(0)))
    ) AS demurraged_balance_raw
FROM snapshots
