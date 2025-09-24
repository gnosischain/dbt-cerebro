WITH tx AS (
  SELECT
      toStartOfDay(block_timestamp)                                  AS date,
      toString(transaction_type)                                      AS transaction_type,
      success,
      COUNT(*)                                                        AS n_txs,
      SUM(value/POWER(10,18))                                         AS xdai_value,
      AVG(value/POWER(10,18))                                         AS xdai_value_avg,
      median(value/POWER(10,18))                                      AS xdai_value_median,
      SUM(COALESCE(gas_used/POWER(10,9),0))                           AS gas_used,
      CAST(AVG(COALESCE(gas_price/POWER(10,9),0)) AS Int32)           AS gas_price_avg,
      CAST(median(COALESCE(gas_price/POWER(10,9),0)) AS Int32)        AS gas_price_median,
      SUM(toFloat64(gas_used) * toFloat64(gas_price)) / 1e18          AS fee_native_sum     
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp < TODAY()
  {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
  GROUP BY 1,2,3
),
px AS (
  SELECT
    price_date,
    anyLast(price_usd) AS price_usd
  FROM {{ ref('stg_execution_transactions__prices') }}
  GROUP BY price_date
)
SELECT
    tx.date,
    tx.transaction_type,
    tx.success,
    tx.n_txs,
    tx.xdai_value,
    tx.xdai_value_avg,
    tx.xdai_value_median,
    tx.gas_used,
    tx.gas_price_avg,
    tx.gas_price_median,
    tx.fee_native_sum,
    tx.fee_native_sum * COALESCE(px.price_usd, 1.0) AS fee_usd_sum   
FROM tx
LEFT JOIN px
  ON px.price_date = toDate(tx.date)