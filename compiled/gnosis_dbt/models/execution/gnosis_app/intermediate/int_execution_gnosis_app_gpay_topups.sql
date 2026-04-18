



WITH ga_users AS (
    SELECT address FROM `dbt`.`int_execution_gnosis_app_users_current`
),

-- GA-user CoW trades in the incremental window.
ga_trades AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.transaction_hash,
        t.log_index,
        t.taker,
        t.order_uid,
        t.token_bought_address,
        t.token_bought_symbol,
        t.amount_bought,
        t.amount_bought_raw,
        t.token_sold_address,
        t.token_sold_symbol,
        t.amount_sold,
        t.amount_sold_raw,
        t.amount_usd,
        t.solver
    FROM `dbt`.`int_execution_cow_trades` t
    WHERE t.taker IN (SELECT address FROM ga_users)
      AND t.block_timestamp >= toDateTime('2025-11-12')
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(t.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_gnosis_app_gpay_topups` AS x1
      WHERE 1=1 
    )
    AND toDate(t.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_gnosis_app_gpay_topups` AS x2
      WHERE 1=1 
    )
  

      
),

-- GP Safe deposits ("Crypto Deposit" action) in the same window.
-- tx_hash is 0x-prefixed on both sides, so the join is direct.
gp_deposits AS (
    SELECT
        a.transaction_hash                      AS transaction_hash,
        a.wallet_address                        AS gp_wallet,
        a.token_address                         AS token_address,
        a.symbol                                AS token_received_symbol,
        a.amount                                AS amount_received,
        a.amount_usd                            AS amount_received_usd
    FROM `dbt`.`int_execution_gpay_activity` a
    WHERE a.action = 'Crypto Deposit'
      AND a.block_timestamp >= toDateTime('2025-11-12')
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(a.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_gnosis_app_gpay_topups` AS x1
      WHERE 1=1 
    )
    AND toDate(a.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_gnosis_app_gpay_topups` AS x2
      WHERE 1=1 
    )
  

      
)

SELECT
    t.block_number                   AS block_number,
    t.block_timestamp                AS block_timestamp,
    t.transaction_hash               AS transaction_hash,
    t.log_index                      AS log_index,
    t.taker                          AS ga_user,
    d.gp_wallet                      AS gp_wallet,
    t.order_uid                      AS order_uid,
    t.token_sold_address             AS token_sold_address,
    t.token_sold_symbol              AS token_sold_symbol,
    t.amount_sold                    AS amount_sold,
    t.token_bought_address           AS token_bought_address,
    t.token_bought_symbol            AS token_bought_symbol,
    t.amount_bought                  AS amount_bought,
    coalesce(t.amount_usd,
             d.amount_received_usd)  AS amount_usd,
    t.solver                         AS solver
FROM ga_trades t
INNER JOIN gp_deposits d
    ON d.transaction_hash = t.transaction_hash
   AND lower(d.token_address) = lower(t.token_bought_address)