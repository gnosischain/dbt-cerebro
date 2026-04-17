






WITH ga_users AS (
    SELECT address FROM `dbt`.`int_execution_gnosis_app_users_current`
),

relayer_addrs AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM `dbt`.`gnosis_app_relayers`
    WHERE is_active = 1
),

-- PreSignature events from GPv2Settlement, scoped to the Cometh bundler
-- routing pattern. signed=true excludes revocations.
cometh_presignatures AS (
    SELECT
        e.block_number                                      AS block_number,
        e.block_timestamp                                   AS block_timestamp,
        e.transaction_hash                                  AS transaction_hash,
        e.log_index                                         AS log_index,
        lower(e.decoded_params['owner'])                    AS taker,
        e.decoded_params['orderUid']                        AS order_uid,
        tx.from_address                                     AS relayer_address
    FROM `dbt`.`contracts_CowProtocol_GPv2Settlement_events` e
    INNER JOIN `execution`.`transactions` tx
        ON tx.transaction_hash = e.transaction_hash
    WHERE e.event_name = 'PreSignature'
      -- decode_logs renders the `signed` bool as '1' / '0' string, not 'true' / 'false'.
      AND e.decoded_params['signed'] = '1'
      AND tx.to_address = '0000000071727de22e5e9d8baf0edac6f37da032'
      AND lower(tx.from_address) IN (SELECT addr FROM relayer_addrs)
      AND e.block_timestamp >= toDateTime('2025-11-12')
      AND tx.block_timestamp >= toDateTime('2025-11-12')
      AND lower(e.decoded_params['owner']) IN (SELECT address FROM ga_users)
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(e.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_gnosis_app_swaps` AS x1
      WHERE 1=1 
    )
    AND toDate(e.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_gnosis_app_swaps` AS x2
      WHERE 1=1 
    )
  

      
),

-- One row per (taker, order_uid): aggregated trade fills.
-- A single order can fill in multiple Trade events (partial fills);
-- collapse them here so the LEFT JOIN stays one-to-one with presignatures.
trade_rollup AS (
    SELECT
        taker,
        order_uid,
        min(block_timestamp)                                AS first_fill_at,
        any(block_number)                                   AS first_fill_block,
        argMin(transaction_hash, block_timestamp)           AS first_fill_tx,
        argMin(token_bought_address, block_timestamp)       AS token_bought_address,
        argMin(token_bought_symbol,  block_timestamp)       AS token_bought_symbol,
        argMin(token_sold_address,   block_timestamp)       AS token_sold_address,
        argMin(token_sold_symbol,    block_timestamp)       AS token_sold_symbol,
        sum(amount_bought_raw)                              AS amount_bought_raw,
        sum(amount_bought)                                  AS amount_bought,
        sum(amount_sold_raw)                                AS amount_sold_raw,
        sum(amount_sold)                                    AS amount_sold,
        sum(fee_amount_raw)                                 AS fee_amount_raw,
        sum(fee_amount)                                     AS fee_amount,
        sum(amount_usd)                                     AS amount_usd,
        any(solver)                                         AS solver,
        count(*)                                            AS n_fills
    FROM `dbt`.`int_execution_cow_trades`
    WHERE block_timestamp >= toDateTime('2025-11-12')
      
    GROUP BY taker, order_uid
)

SELECT
    p.block_number                              AS block_number,
    p.block_timestamp                           AS block_timestamp,
    -- contracts_CowProtocol_GPv2Settlement_events carries tx_hash without
    -- the 0x prefix; prefix at output to match the repo-wide convention
    -- (addresses and tx hashes are emitted 0x-prefixed, lowercase).
    concat('0x', p.transaction_hash)            AS transaction_hash,
    p.log_index                                 AS log_index,
    p.taker                                     AS taker,
    p.order_uid                                 AS order_uid,
    -- execution.transactions stores from_address without the 0x prefix.
    concat('0x', p.relayer_address)             AS relayer_address,
    t.first_fill_at IS NOT NULL                 AS was_filled,
    t.first_fill_at                             AS first_fill_at,
    t.first_fill_tx                             AS first_fill_tx,
    t.n_fills                                   AS n_fills,
    t.token_bought_address                      AS token_bought_address,
    t.token_bought_symbol                       AS token_bought_symbol,
    t.amount_bought_raw                         AS amount_bought_raw,
    t.amount_bought                             AS amount_bought,
    t.token_sold_address                        AS token_sold_address,
    t.token_sold_symbol                         AS token_sold_symbol,
    t.amount_sold_raw                           AS amount_sold_raw,
    t.amount_sold                               AS amount_sold,
    t.fee_amount_raw                            AS fee_amount_raw,
    t.fee_amount                                AS fee_amount,
    t.amount_usd                                AS amount_usd,
    t.solver                                    AS solver
FROM cometh_presignatures p
LEFT JOIN trade_rollup t
    ON t.taker     = p.taker
   AND t.order_uid = p.order_uid