{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH trust_intervals AS (
    SELECT
        group_address,
        toInt64(toUnixTimestamp(valid_from)) AS start_ts,
        if(valid_to IS NULL, CAST(NULL AS Nullable(Int64)), toInt64(toUnixTimestamp(valid_to))) AS end_ts
    FROM {{ ref('int_execution_circles_group_membership_timeline') }}
),
membership_changes AS (
    SELECT
        group_address,
        toStartOfDay(toDateTime(start_ts)) AS timestamp,
        1 AS delta
    FROM trust_intervals

    UNION ALL

    SELECT
        group_address,
        toStartOfDay(toDateTime(end_ts)) AS timestamp,
        -1 AS delta
    FROM trust_intervals
    WHERE end_ts IS NOT NULL
),
changes_daily AS (
    SELECT
        group_address,
        timestamp,
        sum(delta) AS delta
    FROM membership_changes
    GROUP BY 1, 2
),
range_per_group AS (
    SELECT
        group_address,
        min(timestamp) AS min_ts,
        toStartOfDay(toDateTime({{ circles_chain_now_ts() }})) AS max_ts
    FROM changes_daily
    GROUP BY 1
),
calendar AS (
    SELECT
        group_address,
        toDateTime(min_ts) + number * 86400 AS timestamp
    FROM range_per_group
    ARRAY JOIN range(dateDiff('day', min_ts, max_ts) + 1) AS number
),
dense AS (
    SELECT
        c.group_address,
        c.timestamp,
        coalesce(cd.delta, 0) AS delta
    FROM calendar c
    LEFT JOIN changes_daily cd
        ON c.group_address = cd.group_address
       AND c.timestamp = cd.timestamp
)

SELECT
    toDate(timestamp) AS date,
    group_address,
    sum(delta) OVER (PARTITION BY group_address ORDER BY timestamp) AS member_count
FROM dense
