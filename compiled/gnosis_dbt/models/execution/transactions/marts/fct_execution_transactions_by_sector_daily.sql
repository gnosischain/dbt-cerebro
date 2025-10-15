


SELECT
    date,
    sector,
    groupBitmapMerge(ua_bitmap_state)                 AS active_accounts,
    sum(tx_count)                                     AS txs,
    sum(gas_used_sum)                                 AS gas_used_sum,
    round(toFloat64(sum(fee_native_sum)), 6)          AS fee_native_sum
FROM `dbt`.`int_execution_transactions_by_project_daily`
GROUP BY
  date, sector