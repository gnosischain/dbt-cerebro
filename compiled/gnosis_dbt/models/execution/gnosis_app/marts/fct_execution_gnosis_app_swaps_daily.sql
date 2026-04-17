

SELECT
    toDate(block_timestamp)                      AS date,
    count(*)                                     AS n_swaps,
    countIf(was_filled)                          AS n_swaps_filled,
    countIf(NOT was_filled)                      AS n_swaps_unfilled,
    countDistinct(taker)                         AS n_swappers,
    countDistinct(order_uid)                     AS n_orders,
    sum(amount_usd)                              AS volume_usd_filled,
    sumIf(amount_usd, amount_usd IS NOT NULL)    AS volume_usd_priced,
    countIf(was_filled AND amount_usd IS NULL)   AS n_filled_unpriced
FROM `dbt`.`int_execution_gnosis_app_swaps`
GROUP BY toDate(block_timestamp)
ORDER BY date