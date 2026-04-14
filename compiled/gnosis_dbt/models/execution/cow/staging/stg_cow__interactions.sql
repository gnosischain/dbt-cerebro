




SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    lower(decoded_params['target'])                                          AS target,
    decoded_params['value']                                                  AS value,
    decoded_params['selector']                                               AS selector
FROM `dbt`.`contracts_CowProtocol_GPv2Settlement_events` e
WHERE e.event_name = 'Interaction'
  AND e.block_timestamp < today()
  