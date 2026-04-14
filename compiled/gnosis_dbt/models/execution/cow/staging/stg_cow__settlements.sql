




SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    lower(decoded_params['solver'])                                          AS solver
FROM `dbt`.`contracts_CowProtocol_GPv2Settlement_events` e
WHERE e.event_name = 'Settlement'
  AND e.block_timestamp < today()
  AND decoded_params['solver'] IS NOT NULL
  