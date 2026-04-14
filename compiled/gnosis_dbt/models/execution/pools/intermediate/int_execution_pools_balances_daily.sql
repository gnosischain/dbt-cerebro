SELECT * FROM `dbt`.`int_execution_pools_uniswap_v3_daily`

UNION ALL

SELECT * FROM `dbt`.`int_execution_pools_swapr_v3_daily`

UNION ALL

SELECT * FROM `dbt`.`int_execution_pools_balancer_v2_daily`

UNION ALL

SELECT * FROM `dbt`.`int_execution_pools_balancer_v3_daily`