{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_batch_metrics_ts','granularity:daily']
  )
}}

WITH base AS (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        countIf(num_interactions = 0 AND num_trades > 1)                             AS pure_cow,
        countIf(num_trades > 1 AND num_interactions > 0
                AND num_trades > num_interactions)                                   AS partial_cow,
        countIf((num_trades = 1)
                OR (num_trades > 1 AND num_trades <= num_interactions))              AS pure_dex,
        count()                                                                      AS total
    FROM {{ ref('int_execution_cow_batches') }}
    WHERE toDate(block_timestamp) < today()
    GROUP BY date
)
SELECT * FROM (
    SELECT date, 'Pure CoW'    AS label, round(pure_cow    / total * 100, 2) AS value FROM base
    UNION ALL
    SELECT date, 'Partial CoW' AS label, round(partial_cow / total * 100, 2) AS value FROM base
    UNION ALL
    SELECT date, 'Pure DEX'    AS label, round(pure_dex    / total * 100, 2) AS value FROM base
)
ORDER BY date, label
