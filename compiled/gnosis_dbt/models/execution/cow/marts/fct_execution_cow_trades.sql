




WITH

trades AS (
    SELECT *
    FROM `dbt`.`int_execution_cow_trades`
    
      
  
    
    

   WHERE 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`fct_execution_cow_trades` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`fct_execution_cow_trades` AS x2
      WHERE 1=1 
    )
  

    
),

api_fees AS (
    SELECT
        order_uid,
        fee_token,
        fee_amount
    FROM `dbt`.`stg_crawlers_data__cow_api_trade_fees`
)

SELECT
    t.block_number                                                                   AS block_number,
    t.block_timestamp                                                                AS block_timestamp,
    t.transaction_hash                                                               AS transaction_hash,
    t.log_index                                                                      AS log_index,
    t.protocol                                                                       AS protocol,
    t.pool_address                                                                   AS pool_address,
    t.token_bought_address                                                           AS token_bought_address,
    t.token_bought_symbol                                                            AS token_bought_symbol,
    t.amount_bought_raw                                                              AS amount_bought_raw,
    t.amount_bought                                                                  AS amount_bought,
    t.token_sold_address                                                             AS token_sold_address,
    t.token_sold_symbol                                                              AS token_sold_symbol,
    t.amount_sold_raw                                                                AS amount_sold_raw,
    t.amount_sold                                                                    AS amount_sold,
    t.amount_usd                                                                     AS amount_usd,
    t.fee_amount_raw                                                                 AS fee_amount_raw,
    t.fee_amount                                                                     AS fee_amount,
    f.fee_token                                                                      AS api_fee_token,
    f.fee_amount                                                                     AS api_fee_amount_raw,
    COALESCE(
        CASE WHEN t.fee_amount_raw > 0
             THEN t.amount_usd * toFloat64(t.fee_amount_raw) / nullIf(toFloat64(t.amount_sold_raw), 0)
        END,
        CASE
            WHEN f.fee_token = t.token_sold_address
            THEN t.amount_usd * toFloat64(f.fee_amount) / nullIf(toFloat64(t.amount_sold_raw), 0)
            WHEN f.fee_token = t.token_bought_address
            THEN t.amount_usd * toFloat64(f.fee_amount) / nullIf(toFloat64(t.amount_bought_raw), 0)
        END
    )                                                                                AS fee_usd,
    multiIf(
        t.fee_amount_raw > 0,             'onchain',
        f.fee_token != '',                'api',
        NULL
    )                                                                                AS fee_source,
    t.taker                                                                          AS taker,
    t.order_uid                                                                      AS order_uid,
    t.solver                                                                         AS solver
FROM trades t
LEFT JOIN api_fees f
    ON f.order_uid = t.order_uid