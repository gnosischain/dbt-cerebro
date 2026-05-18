{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='date',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'backing', 'daily']
    )
}}

-- Daily cumulative count of trust-defined backers (addresses ever trusted
-- by the backers group). Each row exposes new_backers (first-trusted-at
-- on this date) and the running total.
--
-- Dense calendar: every day from circles_target_group_start_date through
-- yesterday is emitted, even on zero-new-backer days, so downstream
-- charts have a continuous time axis.

WITH per_day AS (
    SELECT
        toDate(first_trusted_at) AS date,
        count()                   AS new_backers
    FROM {{ ref('int_execution_circles_v2_backers_current') }}
    GROUP BY date
),

calendar AS (
    SELECT
        addDays(toDate('{{ var("circles_target_group_start_date") }}'), n) AS date
    FROM (
        SELECT range(toUInt32(dateDiff(
            'day',
            toDate('{{ var("circles_target_group_start_date") }}'),
            yesterday()
        ) + 1)) AS r
    )
    ARRAY JOIN r AS n
),

dense AS (
    SELECT
        c.date                            AS date,
        coalesce(p.new_backers, toUInt64(0)) AS new_backers
    FROM calendar c
    LEFT JOIN per_day p ON p.date = c.date
)

SELECT
    date,
    new_backers,
    sum(new_backers) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_backers
FROM dense
ORDER BY date
