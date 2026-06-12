

-- Hop-count distribution by time window. Each branch computes percentages over its
-- own window so the denominator is window-scoped. Dashboard filters by time_window.

SELECT '1m' AS time_window,
    hop_bucket                                                                  AS label,
    round(100.0 * count() / (
        SELECT count() FROM `dbt`.`int_execution_trades_by_tx`
        WHERE date >= today() - INTERVAL 30 DAY AND date < today()
    ), 2)                                                                       AS value,
    multiIf(
        hop_bucket = '1 hop',  1,
        hop_bucket = '2 hops', 2,
        hop_bucket = '3 hops', 3,
                               4
    )                                                                           AS bucket_order
FROM `dbt`.`int_execution_trades_by_tx`
WHERE date >= today() - INTERVAL 30 DAY AND date < today()
GROUP BY hop_bucket

UNION ALL

SELECT '3m' AS time_window,
    hop_bucket                                                                  AS label,
    round(100.0 * count() / (
        SELECT count() FROM `dbt`.`int_execution_trades_by_tx`
        WHERE date >= today() - INTERVAL 90 DAY AND date < today()
    ), 2)                                                                       AS value,
    multiIf(
        hop_bucket = '1 hop',  1,
        hop_bucket = '2 hops', 2,
        hop_bucket = '3 hops', 3,
                               4
    )                                                                           AS bucket_order
FROM `dbt`.`int_execution_trades_by_tx`
WHERE date >= today() - INTERVAL 90 DAY AND date < today()
GROUP BY hop_bucket

UNION ALL

SELECT '6m' AS time_window,
    hop_bucket                                                                  AS label,
    round(100.0 * count() / (
        SELECT count() FROM `dbt`.`int_execution_trades_by_tx`
        WHERE date >= today() - INTERVAL 180 DAY AND date < today()
    ), 2)                                                                       AS value,
    multiIf(
        hop_bucket = '1 hop',  1,
        hop_bucket = '2 hops', 2,
        hop_bucket = '3 hops', 3,
                               4
    )                                                                           AS bucket_order
FROM `dbt`.`int_execution_trades_by_tx`
WHERE date >= today() - INTERVAL 180 DAY AND date < today()
GROUP BY hop_bucket

UNION ALL

SELECT '1y' AS time_window,
    hop_bucket                                                                  AS label,
    round(100.0 * count() / (
        SELECT count() FROM `dbt`.`int_execution_trades_by_tx`
        WHERE date >= today() - INTERVAL 365 DAY AND date < today()
    ), 2)                                                                       AS value,
    multiIf(
        hop_bucket = '1 hop',  1,
        hop_bucket = '2 hops', 2,
        hop_bucket = '3 hops', 3,
                               4
    )                                                                           AS bucket_order
FROM `dbt`.`int_execution_trades_by_tx`
WHERE date >= today() - INTERVAL 365 DAY AND date < today()
GROUP BY hop_bucket