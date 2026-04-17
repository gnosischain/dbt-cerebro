

WITH gp_roles_modules AS (
    SELECT gp_safe, module_proxy_address AS roles_module_address
    FROM `dbt`.`int_execution_gpay_safe_modules`
    WHERE contract_type = 'RolesModule'
),

allowance_latest AS (
    SELECT
        roles_module_address,
        allowance_key,
        argMax(balance,         (block_number, log_index)) AS balance,
        argMax(max_refill,      (block_number, log_index)) AS max_refill,
        argMax(refill,          (block_number, log_index)) AS refill,
        argMax(period,          (block_number, log_index)) AS period,
        argMax(block_timestamp, (block_number, log_index)) AS last_set_at
    FROM `dbt`.`int_execution_gpay_roles_events`
    WHERE event_name = 'SetAllowance'
      AND allowance_key IS NOT NULL
    GROUP BY roles_module_address, allowance_key
)

SELECT
    rm.gp_safe,
    a.allowance_key,
    a.balance,
    a.max_refill,
    a.refill,
    a.period,
    a.last_set_at
FROM allowance_latest a
INNER JOIN gp_roles_modules rm
    ON rm.roles_module_address = a.roles_module_address