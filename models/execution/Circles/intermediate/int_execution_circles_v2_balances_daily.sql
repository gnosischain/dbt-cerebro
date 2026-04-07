{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, token_address, account)',
        unique_key='(date, token_address, account)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'balances']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
WITH deltas AS (
    SELECT
        date,
        account,
        token_address,
        sum(delta_raw) AS net_delta_raw,
        max(last_activity_ts) AS last_activity_ts_for_day
    FROM {{ ref('int_execution_circles_v2_balance_diffs_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=2) }}
      {% endif %}
    GROUP BY 1, 2, 3
),
overall_max_date AS (
    SELECT
        {% if end_month %}
            least(toLastDayOfMonth(toDate('{{ end_month }}')), yesterday())
        {% else %}
            yesterday()
        {% endif %} AS max_date
),
{% if is_incremental() %}
current_partition AS (
    SELECT
        max(date) AS max_date
    FROM {{ this }}
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        account,
        token_address,
        balance_raw,
        last_activity_ts
    FROM {{ this }}
    WHERE date = (SELECT max_date FROM current_partition)
),
keys AS (
    SELECT DISTINCT account, token_address
    FROM (
        SELECT account, token_address FROM prev_balances
        UNION ALL
        SELECT account, token_address FROM deltas
    )
),
calendar AS (
    SELECT
        k.account,
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
        account,
        token_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            account,
            token_address,
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
        c.date AS date,
        c.account AS account,
        c.token_address AS token_address,
        sum(coalesce(d.net_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.account, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %} AS balance_raw,
        toUInt64(
            greatest(
                max(coalesce(d.last_activity_ts_for_day, toUInt64(0))) OVER (
                    PARTITION BY c.account, c.token_address
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
     AND c.account = d.account
     AND c.token_address = d.token_address
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
      ON c.account = p.account
     AND c.token_address = p.token_address
    {% endif %}
),
snapshots AS (
    SELECT
        date,
        account,
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
    account,
    token_address,
    balance_raw,
    last_activity_ts,
    snapshot_ts,
    toInt256(
        multiplyDecimal(
            toDecimal256(balance_raw, 0),
            {{ circles_demurrage_factor('last_activity_ts', 'snapshot_ts') }},
            0
        )
    ) AS demurraged_balance_raw
FROM snapshots
