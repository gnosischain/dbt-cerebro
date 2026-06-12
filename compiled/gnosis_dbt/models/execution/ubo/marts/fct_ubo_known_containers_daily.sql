


SELECT DISTINCT
    date,
    container_address,
    token_address
FROM `dbt`.`fct_ubo_supply_claims_daily`
WHERE date < today()