{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_top_pairs_weekly','granularity:weekly']
  )
}}

WITH pair_weekly AS (
    SELECT
        toStartOfWeek(block_timestamp, 1)                                        AS date,
        concat(token_sold_symbol, ' → ', token_bought_symbol)                    AS pair,
        sum(amount_usd)                                                          AS volume_usd
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE toDate(block_timestamp) < today()
    GROUP BY date, pair
),
top_pairs AS (
    SELECT pair
    FROM pair_weekly
    GROUP BY pair
    ORDER BY sum(volume_usd) DESC
    LIMIT 8
)
SELECT
    pw.date                                                                      AS date,
    CASE
        WHEN pw.pair IN (SELECT pair FROM top_pairs) THEN pw.pair
        ELSE 'Other'
    END                                                                          AS label,
    sum(pw.volume_usd)                                                           AS value
FROM pair_weekly pw
GROUP BY date, label
ORDER BY date, label
