







WITH traces AS (
    

SELECT action_from, action_to, action_input, action_call_type, result_gas_used, block_timestamp, block_number, transaction_hash
FROM (
    SELECT
        action_from, action_to, action_input, action_call_type, result_gas_used, block_timestamp, block_number, transaction_hash,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_hash, trace_address
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`traces`
    
    WHERE 
    action_call_type = 'delegate_call'
    AND result_gas_used > 10000
    AND lower(substring(action_input, 1, 8)) IN ('0ec78d9e','a97ab18a','b63e800d')
    AND lower(action_to) IN (
        SELECT lower(replaceAll(address, '0x', '')) FROM `dbt`.`safe_singletons`
    )
    AND block_timestamp >= toDateTime('2020-05-21')
    
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_safes` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_safes` AS x2
      WHERE 1=1 
    )
  

    

    
)
WHERE _dedup_rn = 1

),

singletons AS (
    -- Match the trace storage format: lowercase hex, NO 0x prefix.
    SELECT
        lower(replaceAll(address, '0x', ''))        AS singleton_address,
        version,
        is_l2,
        lower(replaceAll(setup_selector, '0x', '')) AS setup_selector
    FROM `dbt`.`safe_singletons`
)

SELECT
    -- Re-prefix outputs so downstream models get the canonical 0x... shape.
    concat('0x', lower(tr.action_from))              AS safe_address,
    sg.version                                       AS creation_version,
    sg.is_l2                                         AS is_l2,
    concat('0x', lower(tr.action_to))                AS creation_singleton,
    toDate(tr.block_timestamp)                       AS block_date,
    tr.block_timestamp                               AS block_timestamp,
    tr.block_number                                  AS block_number,
    concat('0x',tr.transaction_hash)                 AS tx_hash,
    tr.result_gas_used                               AS gas_used
FROM traces tr
INNER JOIN singletons sg
    ON lower(tr.action_to) = sg.singleton_address
   AND lower(substring(tr.action_input, 1, 8)) = sg.setup_selector