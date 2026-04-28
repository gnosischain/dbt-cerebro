




WITH

trades AS (
    SELECT
        block_timestamp,
        transaction_hash,
        amount_usd,
        solver
    FROM `dbt`.`int_execution_cow_trades`
    
      
  
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_cow_batches` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_cow_batches` AS x2
        WHERE 1=1 
      )
    
  

    
),

interactions AS (
    SELECT
        transaction_hash,
        count(*) AS num_interactions
    FROM `dbt`.`stg_cow__interactions`
    
    WHERE block_timestamp >= (SELECT addDays(max(toDate(block_timestamp)), -3) FROM `dbt`.`int_execution_cow_batches`)
    
    GROUP BY transaction_hash
),

batch_trades AS (
    SELECT
        min(block_timestamp)                                                         AS block_timestamp,
        transaction_hash,
        any(solver)                                                                  AS solver,
        count(*)                                                                     AS num_trades,
        countDistinct(amount_usd)                                                    AS num_priced_trades,
        sum(amount_usd)                                                              AS batch_value_usd
    FROM trades
    GROUP BY transaction_hash
),

tx_context AS (
    SELECT
        concat('0x', transaction_hash) AS transaction_hash,
        gas_used,
        gas_price
    FROM `execution`.`transactions`
    WHERE replaceAll(lower(to_address), '0x', '') = '9008d19f58aabd9ed0d60971565aa8510560ab41'
    
      AND block_timestamp >= (
          SELECT addDays(max(toDate(block_timestamp)), -3)
          FROM `dbt`.`int_execution_cow_batches`
      )
    
)

SELECT
    bt.block_timestamp                                                               AS block_timestamp,
    bt.transaction_hash                                                              AS transaction_hash,
    bt.solver                                                                        AS solver,
    bt.num_trades                                                                    AS num_trades,
    coalesce(i.num_interactions, 0)                                                  AS num_interactions,
    coalesce(i.num_interactions, 0) = 0 AND bt.num_trades > 1                        AS is_cow,
    bt.batch_value_usd                                                               AS batch_value_usd,
    tx.gas_used                                                                      AS gas_used,
    tx.gas_price                                                                     AS gas_price,
    toFloat64(tx.gas_used) * toFloat64(tx.gas_price) / 1e18                          AS tx_cost_native
FROM batch_trades bt
LEFT JOIN interactions i
    ON i.transaction_hash = bt.transaction_hash
LEFT JOIN tx_context tx
    ON tx.transaction_hash = bt.transaction_hash