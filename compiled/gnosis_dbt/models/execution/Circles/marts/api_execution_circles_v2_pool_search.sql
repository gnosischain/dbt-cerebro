
-- (pool_address, display_name) lookup that backs the Pool Explorer filter dropdown.
SELECT today() AS as_of_date, lower(pool_address) AS pool_address, label AS display_name
FROM `dbt`.`circles_liquidity_pools`