{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='date',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'trusts']
    )
}}

WITH exploded AS (
    SELECT
        truster,
        trustee,
        r.1 AS valid_from,
        r.2 AS valid_to
    FROM {{ ref('int_execution_circles_v2_trust_pair_ranges') }}
    ARRAY JOIN arrayZip(valid_from_agg, valid_to_agg) AS r
),
daily_deltas AS (
    -- +1 on valid_from (trust created)
    SELECT
        toDate(valid_from) AS date,
        toInt64(1) AS delta
    FROM exploded

    UNION ALL

    -- -1 on valid_to (trust revoked/expired)
    SELECT
        toDate(valid_to) AS date,
        toInt64(-1) AS delta
    FROM exploded
    WHERE valid_to < toDateTime('2106-02-07 06:28:15')
),
collapsed AS (
    SELECT
        date,
        sumIf(delta, delta > 0) AS new_trusts,
        -sumIf(delta, delta < 0) AS revoked_trusts,
        sum(delta) AS net_delta
    FROM daily_deltas
    GROUP BY date
),
bounds AS (
    SELECT min(date) AS min_date, today() AS max_date FROM collapsed
),
calendar AS (
    SELECT addDays(b.min_date, n) AS date
    FROM bounds b
    ARRAY JOIN range(toUInt32(dateDiff('day', b.min_date, b.max_date) + 1)) AS n
),
dense AS (
    SELECT
        c.date,
        coalesce(d.new_trusts, toInt64(0)) AS new_trusts,
        coalesce(d.revoked_trusts, toInt64(0)) AS revoked_trusts,
        coalesce(d.net_delta, toInt64(0)) AS net_delta
    FROM calendar c
    LEFT JOIN collapsed d ON c.date = d.date
)

SELECT
    date,
    new_trusts,
    revoked_trusts,
    sum(net_delta) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS active_trusts
FROM dense
ORDER BY date
