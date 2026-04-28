

SELECT DISTINCT
    lower(decoded_params['poolId'])                                          AS pool_id,
    concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', '')) AS pool_address
FROM `dbt`.`contracts_BalancerV2_Vault_events`
WHERE event_name = 'PoolRegistered'
  AND decoded_params['poolId']      IS NOT NULL
  AND decoded_params['poolAddress'] IS NOT NULL