

-- Latest enable/disable state per (modifier, submodule) over the historical
-- event log. Keeps only pairs whose most recent event is an enable.
WITH submodule_state AS (
    SELECT
        modifier_address,
        submodule_address,
        argMax(event_kind, (block_number, log_index)) AS latest_kind,
        max(block_timestamp)                          AS last_event_at
    FROM `dbt`.`int_execution_zodiac_modifier_module_events`
    WHERE submodule_address IS NOT NULL
    GROUP BY modifier_address, submodule_address
),

enabled AS (
    SELECT modifier_address, submodule_address, last_event_at
    FROM submodule_state
    WHERE latest_kind = 'enabled_module'
),

-- Avatar = the Safe that currently has this Modifier enabled as its module.
-- Derived from the Safe-side module events (not self-referential).
safe_modifier_state AS (
    SELECT
        safe_address,
        target_address                                AS modifier_address,
        argMax(event_kind, (block_number, log_index)) AS latest_kind,
        max((block_number, log_index))                AS order_key
    FROM `dbt`.`int_execution_safes_module_events`
    WHERE target_address IS NOT NULL
    GROUP BY safe_address, target_address
),

modifier_avatar AS (
    SELECT
        modifier_address,
        argMax(safe_address, order_key) AS avatar_address
    FROM safe_modifier_state
    WHERE latest_kind = 'enabled_module'
    GROUP BY modifier_address
),

registry AS (
    SELECT
        address,
        contract_type,
        abi_source_address AS master_copy
    FROM `dbt`.`contracts_zodiac_modules_registry`
),

safes AS (
    SELECT address FROM `dbt`.`contracts_safe_registry`
),

gp_safes AS (
    SELECT address FROM `dbt`.`int_execution_gpay_wallets`
)

SELECT
    e.modifier_address                       AS modifier_address,
    e.submodule_address                      AS submodule_address,
    r.contract_type                          AS module_type,
    r.master_copy                            AS master_copy,
    ma.avatar_address                        AS avatar_address,
    toUInt8(s.address IS NOT NULL)           AS submodule_is_safe,
    toUInt8(s.address IS NOT NULL)           AS is_erc1271_exploitable,
    toUInt8(gp.address IS NOT NULL)          AS is_gp,
    e.last_event_at                          AS last_event_at
FROM enabled e
LEFT JOIN registry r       ON r.address = e.modifier_address
LEFT JOIN modifier_avatar ma ON ma.modifier_address = e.modifier_address
LEFT JOIN safes s          ON s.address = e.submodule_address
LEFT JOIN gp_safes gp      ON gp.address = ma.avatar_address