

-- Hop-count distribution over the last 30 days. Light GROUP BY on the
-- pre-bucketed hop_bucket column in int_execution_trades_by_tx.

WITH

recent AS (
    SELECT hop_bucket
    FROM `dbt`.`int_execution_trades_by_tx`
    WHERE date >= today() - INTERVAL 30 DAY
      AND date <  today()
),

total AS (
    SELECT count() AS n FROM recent
)

SELECT
    hop_bucket                                                                  AS label,
    count()                                                                     AS trade_count,
    round(100.0 * count() / (SELECT n FROM total), 2)                           AS value,
    multiIf(
        hop_bucket = '1 hop',  1,
        hop_bucket = '2 hops', 2,
        hop_bucket = '3 hops', 3,
                               4
    )                                                                           AS bucket_order
FROM recent
GROUP BY hop_bucket
ORDER BY bucket_order