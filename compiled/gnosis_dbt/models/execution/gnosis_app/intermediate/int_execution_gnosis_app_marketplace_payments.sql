





WITH ga_users AS (
    SELECT address FROM `dbt`.`int_execution_gnosis_app_users_current`
),

relayer_addrs AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM `dbt`.`gnosis_app_relayers`
    WHERE is_active = 1
),

offers AS (
    SELECT gateway_address, offer_name
    FROM `dbt`.`int_execution_gnosis_app_marketplace_offers`
),

payment_events AS (
    SELECT
        e.block_number,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        lower(e.decoded_params['payer'])            AS payer,
        lower(e.decoded_params['payee'])            AS payee,
        lower(e.decoded_params['gateway'])          AS gateway_address,
        toUInt256OrNull(e.decoded_params['tokenId']) AS token_id,
        toUInt256OrNull(e.decoded_params['amount']) AS amount_raw
    FROM `dbt`.`contracts_circles_v2_PaymentGatewayFactory_events` e
    WHERE e.event_name = 'PaymentReceived'
      AND e.block_timestamp >= toDateTime('2025-11-12')
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(e.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_gnosis_app_marketplace_payments` AS x1
      WHERE 1=1 
    )
    AND toDate(e.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_gnosis_app_marketplace_payments` AS x2
      WHERE 1=1 
    )
  

      
),

cometh_txs AS (
    SELECT
        transaction_hash,
        from_address AS relayer_address
    FROM `execution`.`transactions` tx
    WHERE tx.to_address = '0000000071727de22e5e9d8baf0edac6f37da032'
      AND lower(tx.from_address) IN (SELECT addr FROM relayer_addrs)
      AND tx.block_timestamp >= toDateTime('2025-11-12')
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(tx.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_gnosis_app_marketplace_payments` AS x1
      WHERE 1=1 
    )
    AND toDate(tx.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_gnosis_app_marketplace_payments` AS x2
      WHERE 1=1 
    )
  

      
)

SELECT
    p.block_number                              AS block_number,
    p.block_timestamp                           AS block_timestamp,
    concat('0x', p.transaction_hash)            AS transaction_hash,
    p.log_index                                 AS log_index,
    p.payer                                     AS payer,
    p.payee                                     AS payee,
    p.gateway_address                           AS gateway_address,
    o.offer_name                                AS offer_name,
    p.token_id                                  AS token_id,
    p.amount_raw                                AS amount_raw,
    -- CRC v2 is 1e18 scaled
    toFloat64(p.amount_raw) / 1e18              AS amount,
    concat('0x', ct.relayer_address)            AS relayer_address
FROM payment_events p
INNER JOIN cometh_txs ct
    ON ct.transaction_hash = p.transaction_hash
INNER JOIN offers o
    ON o.gateway_address = p.gateway_address
INNER JOIN ga_users u
    ON u.address = p.payer