



WITH ordered AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        truster,
        trustee,
        trust_limit,
        trust_value,
        updated_at,
        lead(toUnixTimestamp(block_timestamp)) OVER (
            PARTITION BY truster, trustee
            ORDER BY block_number, transaction_index, log_index
        ) AS next_update_ts
    FROM `dbt`.`int_execution_circles_v1_trust_updates`
),
intervalized AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        truster,
        trustee,
        trust_limit,
        trust_value,
        updated_at,
        block_timestamp AS valid_from,
        if(next_update_ts > 0, toDateTime(next_update_ts), CAST(NULL AS Nullable(DateTime))) AS valid_to
    FROM ordered
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    truster,
    trustee,
    trust_value,
    trust_limit,
    valid_from,
    valid_to,
    toUInt8(trust_limit > 0) AS is_active,
    updated_at
FROM intervalized
WHERE valid_to IS NULL OR valid_to > valid_from
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(valid_from)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.valid_from)), -0))
        FROM `dbt`.`int_execution_circles_v1_trust_relations` AS x1
        WHERE 1=1 
      )
      AND toDate(valid_from) >= (
        SELECT
          
            addDays(max(toDate(x2.valid_from)), -0)
          

        FROM `dbt`.`int_execution_circles_v1_trust_relations` AS x2
        WHERE 1=1 
      )
    
  

  