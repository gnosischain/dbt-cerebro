



WITH gp_roles_modules AS (
    SELECT gp_safe, module_proxy_address AS roles_module_address
    FROM `dbt`.`int_execution_gpay_safe_modules`
    WHERE contract_type = 'RolesModule'
),

assign_latest AS (
    SELECT
        roles_module_address,
        member_address,
        role_key,
        argMax(is_member,       (block_number, log_index)) AS last_is_member,
        argMax(block_timestamp, (block_number, log_index)) AS last_assigned_at
    FROM `dbt`.`int_execution_gpay_roles_events`
    WHERE event_name = 'AssignRoles'
      AND member_address IS NOT NULL
      AND role_key IS NOT NULL
    GROUP BY roles_module_address, member_address, role_key
)

SELECT
    rm.gp_safe,
    a.member_address                AS delegate_address,
    a.role_key                      AS role_key,
    a.last_assigned_at              AS assigned_at
FROM assign_latest a
INNER JOIN gp_roles_modules rm
    ON rm.roles_module_address = a.roles_module_address
WHERE a.last_is_member = 1