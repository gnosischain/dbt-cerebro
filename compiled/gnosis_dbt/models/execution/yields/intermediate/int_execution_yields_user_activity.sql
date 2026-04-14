


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

lending_events AS (
    SELECT
        block_timestamp,
        transaction_hash,
        e.log_index,
        'Aave V3'                                          AS protocol,
        lower(decoded_params['reserve'])                   AS position_address,
        lower(
            multiIf(
                event_name = 'Supply',  decoded_params['onBehalfOf'],
                event_name = 'Withdraw', decoded_params['user'],
                event_name = 'Borrow',  decoded_params['onBehalfOf'],
                event_name = 'Repay',   decoded_params['user'],
                decoded_params['user']
            )
        )                                                  AS wallet_address,
        multiIf(
            event_name = 'Supply',   'Supply',
            event_name = 'Withdraw', 'Withdraw',
            event_name = 'Borrow',   'Borrow',
            'Repay'
        )                                                  AS action,
        rm.reserve_symbol                                  AS token_symbol,
        lower(decoded_params['reserve'])                   AS token_address,
        toFloat64(toUInt256OrNull(decoded_params['amount']))
            / power(10, rm.decimals)                       AS amount,
        toFloat64(toUInt256OrNull(decoded_params['amount']))
            / power(10, rm.decimals)
            * coalesce(pr.price, 0)                        AS amount_usd,
        'lending'                                          AS source
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events` e
    INNER JOIN `dbt`.`atoken_reserve_mapping` rm
        ON lower(rm.reserve_address) = lower(decoded_params['reserve'])
    LEFT JOIN `dbt`.`int_execution_token_prices_daily` pr
        ON pr.symbol = rm.reserve_symbol
       AND pr.date   = toDate(e.block_timestamp)
    WHERE event_name IN ('Supply', 'Withdraw', 'Borrow', 'Repay')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    

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