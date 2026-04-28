




WITH

lp_events AS (
    SELECT
        block_timestamp,
        transaction_hash,
        log_index,
        protocol,
        pool_address                                       AS position_address,
        provider                                           AS wallet_address,
        multiIf(
            event_type = 'mint',    'Add Liquidity',
            event_type = 'burn',    'Remove Liquidity',
            'Collect Fees'
        )                                                  AS action,
        token_symbol,
        token_address,
        amount,
        amount_usd,
        'lp'                                               AS source
    FROM `dbt`.`int_execution_pools_dex_liquidity_events`
    WHERE provider IS NOT NULL
      AND provider != ''
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_yields_user_activity` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_yields_user_activity` AS x2
        WHERE 1=1 
      )
    
  

      
),

pool_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM `dbt`.`contracts_spark_Pool_events`
),

lending_events AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        e.protocol                                         AS protocol,
        lower(e.decoded_params['reserve'])                 AS position_address,
        lower(
            multiIf(
                e.event_name = 'Supply',  e.decoded_params['onBehalfOf'],
                e.event_name = 'Withdraw', e.decoded_params['user'],
                e.event_name = 'Borrow',  e.decoded_params['onBehalfOf'],
                e.event_name = 'Repay',   e.decoded_params['user'],
                e.decoded_params['user']
            )
        )                                                  AS wallet_address,
        multiIf(
            e.event_name = 'Supply',   'Supply',
            e.event_name = 'Withdraw', 'Withdraw',
            e.event_name = 'Borrow',   'Borrow',
            'Repay'
        )                                                  AS action,
        rm.reserve_symbol                                  AS token_symbol,
        lower(e.decoded_params['reserve'])                 AS token_address,
        toFloat64(toUInt256OrNull(e.decoded_params['amount']))
            / power(10, rm.decimals)                       AS amount,
        toFloat64(toUInt256OrNull(e.decoded_params['amount']))
            / power(10, rm.decimals)
            * coalesce(pr.price, 0)                        AS amount_usd,
        'lending'                                          AS source
    FROM pool_events_raw e
    INNER JOIN `dbt`.`lending_market_mapping` rm
        ON  rm.protocol             = e.protocol
       AND lower(rm.reserve_address) = lower(e.decoded_params['reserve'])
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        ORDER BY symbol, date
    ) pr
        ON  pr.symbol                 = rm.reserve_symbol
        AND toDate(e.block_timestamp) >= pr.date
    WHERE e.event_name IN ('Supply', 'Withdraw', 'Borrow', 'Repay')
      AND e.decoded_params['reserve'] IS NOT NULL
      AND e.decoded_params['amount'] IS NOT NULL
      AND e.block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(e.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_yields_user_activity` AS x1
        WHERE 1=1 
      )
      AND toDate(e.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_yields_user_activity` AS x2
        WHERE 1=1 
      )
    
  

      
)

SELECT
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    position_address,
    wallet_address,
    action,
    token_symbol,
    token_address,
    round(amount, 6)      AS amount,
    round(amount_usd, 2)  AS amount_usd,
    source
FROM lp_events

UNION ALL

SELECT
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    position_address,
    wallet_address,
    action,
    token_symbol,
    token_address,
    round(amount, 6)      AS amount,
    round(amount_usd, 2)  AS amount_usd,
    source
FROM lending_events