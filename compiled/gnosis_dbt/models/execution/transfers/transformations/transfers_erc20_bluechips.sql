


SELECT
    block_number
    ,block_timestamp
    ,transaction_index
    ,log_index
    ,transaction_hash
    ,address AS token_address
    ,concat('0x',substring(topic1,25,40)) AS "from"
    ,concat('0x',substring(topic2,25,40) ) AS "to"
    ,toString(
        reinterpretAsUInt256(
            reverse(unhex(data))
        )
    ) AS "value"
FROM `execution`.`logs`
WHERE
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND address = 'e91d153e0b41518a2ce8dd3d7944fa863463a97d'
    AND block_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(block_timestamp))
      FROM `dbt`.`transfers_erc20_bluechips`
    )
  
