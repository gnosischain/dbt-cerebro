{{
    config(
        materialized='view',
        tags=['live', 'execution', 'gpay', 'api']
    )
}}

-- Minute-grain GP Payments + Fiat Top Ups over the last 2h.
-- label = action so dashboards can stack Payment vs Fiat Top Up.

WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM {{ ref('int_live__gpay_activity_raw') }}
),

minute AS (
    SELECT
        toStartOfMinute(block_timestamp)        AS date,
        action                                  AS label,
        count()                                 AS event_count,
        round(sum(amount_usd), 0)               AS volume_usd
    FROM {{ ref('int_live__gpay_activity_raw') }} FINAL
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 2 HOUR
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
    GROUP BY date, label
)

SELECT
    date,
    label,
    event_count,
    volume_usd
FROM minute
ORDER BY date, label
