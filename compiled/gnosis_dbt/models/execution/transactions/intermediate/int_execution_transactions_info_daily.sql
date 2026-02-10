



WITH deduped_transactions AS (
    SELECT
        block_timestamp,
        transaction_type,
        success,
        CAST(value_string AS UInt256) AS value,
        gas_used,
        gas_price
    FROM (
        

SELECT block_timestamp, transaction_type, success, value_string, gas_used, gas_price
FROM (
    SELECT
        block_timestamp, transaction_type, success, value_string, gas_used, gas_price,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`transactions`
    
    WHERE 
    block_timestamp < today()
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_transactions_info_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_transactions_info_daily` AS x2
      WHERE 1=1 
    )
  


    
)
WHERE _dedup_rn = 1

    )
),

tx AS (
  SELECT
    block_timestamp,
    toDate(block_timestamp)             AS date,
    toString(transaction_type)          AS transaction_type,
    coalesce(success, 0)                AS success,
    toFloat64(value) / 1e18             AS value_native,
    toFloat64(coalesce(gas_used, 0))    AS gas_used,
    toFloat64(coalesce(gas_price, 0))   AS gas_price
  FROM deduped_transactions
),

agg_base AS (
  SELECT
    date,
    transaction_type,
    success,
    COUNT()                     AS n_txs,
    SUM(value_native)           AS xdai_value_sum_raw,
    AVG(value_native)           AS xdai_value_avg_raw,
    median(value_native)        AS xdai_value_median_raw,
    SUM(gas_used)               AS gas_used_sum_raw,          -- “gas units”
    AVG(gas_price)              AS gas_price_avg_raw_wei,     -- in wei
    median(gas_price)           AS gas_price_med_raw_wei,     -- in wei
    SUM(gas_used * gas_price)   AS fee_sum_raw_wei            -- in wei
  FROM tx
  GROUP BY date, transaction_type, success
),

agg AS (
  SELECT
    date,
    transaction_type,
    success,
    n_txs,
    xdai_value_sum_raw                       AS xdai_value,
    xdai_value_avg_raw                       AS xdai_value_avg,
    xdai_value_median_raw                    AS xdai_value_median,
    gas_used_sum_raw                         AS gas_used,
    CAST(gas_price_avg_raw_wei / 1e9 AS Int32)   AS gas_price_avg,       -- Gwei
    CAST(gas_price_med_raw_wei / 1e9 AS Int32)   AS gas_price_median,    -- Gwei
    fee_sum_raw_wei / 1e18                   AS fee_native_sum          -- xDai
  FROM agg_base
),

px AS (
  SELECT
    date,
    price
  FROM `dbt`.`stg_crawlers_data__dune_prices`
  WHERE symbol = 'XDAI'
  
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_transactions_info_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(date) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_transactions_info_daily` AS x2
      WHERE 1=1 
    )
  

)

SELECT
  a.date,
  a.transaction_type,
  a.success,
  a.n_txs,
  a.xdai_value,
  a.xdai_value_avg,
  a.xdai_value_median,
  a.gas_used,
  a.gas_price_avg,
  a.gas_price_median,
  a.fee_native_sum,
  a.fee_native_sum * coalesce(px.price, 1.0) AS fee_usd_sum
FROM agg a
LEFT JOIN px
  ON px.date = a.date