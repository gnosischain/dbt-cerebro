{{
    config(
        materialized='view',
        tags=['dev', 'live', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Hourly USD volume over the live window (up to 48h), stacked by protocol.
-- Volume is summed per-swap (per hop), so multi-hop trades contribute to
-- every protocol they touch. This is the standard DEX-activity view.

WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM {{ ref('int_live__dex_trades_raw') }}
),

hourly AS (
    SELECT
        toStartOfHour(block_timestamp)          AS date,
        protocol                                AS label,
        round(sum(amount_usd), 0)               AS value
    FROM {{ ref('int_live__dex_trades_raw') }}
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 48 HOUR
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
      AND protocol != ''
      AND amount_usd IS NOT NULL
    GROUP BY date, label
)

SELECT
    date,
    label,
    value
FROM hourly
ORDER BY date, label
