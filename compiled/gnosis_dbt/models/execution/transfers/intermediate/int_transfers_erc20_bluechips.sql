


SELECT
    block_number
    ,block_timestamp
    ,transaction_index
    ,log_index
    ,transaction_hash
    ,token_address
    ,"from"
    ,"to"
    ,"value"
FROM `dbt`.`int_transfers_erc20`
WHERE
    token_address = '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d'
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(block_timestamp))
      FROM `dbt`.`int_transfers_erc20_bluechips`
    )
  
