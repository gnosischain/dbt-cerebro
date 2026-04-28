{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(roles_module_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key="(transaction_hash, log_index, coalesce(role_key, ''), coalesce(member_address, ''))",
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay'],
    pre_hook=["SET allow_experimental_json_type = 1", "SET join_algorithm = 'grace_hash'"],
    post_hook=["SET allow_experimental_json_type = 0", "SET join_algorithm = 'default'"]
  )
}}
WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table         = source('execution','logs'),
            contract_address_ref = ref('contracts_gpay_modules_registry'),
            contract_type_filter = 'RolesModule',
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2023-06-01'
        ) }}
    )
),

-- AssignRoles unrolled: one row per (member_module, role_key, is_member).
-- The decoded `module` topic1 is the address being granted/revoked the
-- roles — typically a spender delegate EOA.
--
-- The pre-filter subquery is necessary because ARRAY JOIN evaluates its
-- range expression BEFORE the outer WHERE clause runs. Without the
-- subquery, non-AssignRoles rows reach the ARRAY JOIN with NULL
-- decoded_params['roleKeys'], and `range(1, toUInt32(JSONLength(NULL)) + 1)`
-- fails with `Illegal (null) value`. Filtering inside the subquery means
-- the ARRAY JOIN only sees rows where roleKeys is guaranteed non-null.
assign_role_rows AS (
    SELECT
        concat('0x', lower(contract_address))                                AS roles_module_address,
        'AssignRoles'                                                        AS event_name,
        lower(decoded_params['module'])                                      AS member_address,
        JSONExtractString(decoded_params['roleKeys'], idx)                   AS role_key,
        toUInt8OrNull(JSONExtractString(decoded_params['memberOf'], idx))    AS is_member,
        CAST(NULL AS Nullable(String))                                       AS allowance_key,
        CAST(NULL AS Nullable(String))                                       AS balance,
        CAST(NULL AS Nullable(String))                                       AS max_refill,
        CAST(NULL AS Nullable(String))                                       AS refill,
        CAST(NULL AS Nullable(String))                                       AS period,
        CAST(NULL AS Nullable(String))                                       AS consumed,
        CAST(NULL AS Nullable(String))                                       AS new_balance,
        CAST(NULL AS Nullable(String))                                       AS default_role_key,
        block_timestamp,
        block_number,
        concat('0x', transaction_hash)                                       AS transaction_hash,
        log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'AssignRoles') d
    ARRAY JOIN range(1, toUInt32(JSONLength(d.decoded_params['roleKeys'])) + 1) AS idx
),

-- set_allowance_rows uses the same pre-filter subquery pattern as
-- assign_role_rows. The subquery is necessary because ClickHouse SELECT
-- aliases SHADOW source columns in WHERE clauses: writing
--   SELECT 'SetAllowance' AS event_name FROM decoded WHERE event_name = 'SetAllowance'
-- evaluates the WHERE against the alias literal ('SetAllowance' = 'SetAllowance'
-- is always TRUE), so every decoded row would pass through. Wrapping the
-- FROM in a subquery that selects `*` (no literal alias) ensures the WHERE
-- runs in a scope where nothing shadows `event_name`.
set_allowance_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                              AS roles_module_address,
        'SetAllowance'                                                       AS event_name,
        CAST(NULL AS Nullable(String))                                       AS member_address,
        CAST(NULL AS Nullable(String))                                       AS role_key,
        CAST(NULL AS Nullable(UInt8))                                        AS is_member,
        d.decoded_params['allowanceKey']                                     AS allowance_key,
        d.decoded_params['balance']                                          AS balance,
        d.decoded_params['maxRefill']                                        AS max_refill,
        d.decoded_params['refill']                                           AS refill,
        d.decoded_params['period']                                           AS period,
        CAST(NULL AS Nullable(String))                                       AS consumed,
        CAST(NULL AS Nullable(String))                                       AS new_balance,
        CAST(NULL AS Nullable(String))                                       AS default_role_key,
        d.block_timestamp,
        d.block_number,
        concat('0x', d.transaction_hash)                                     AS transaction_hash,
        d.log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'SetAllowance') d
),

-- consume_allowance_rows uses the same pre-filter subquery pattern.
-- See the comment on set_allowance_rows above for why this is required.
consume_allowance_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                              AS roles_module_address,
        'ConsumeAllowance'                                                   AS event_name,
        CAST(NULL AS Nullable(String))                                       AS member_address,
        CAST(NULL AS Nullable(String))                                       AS role_key,
        CAST(NULL AS Nullable(UInt8))                                        AS is_member,
        d.decoded_params['allowanceKey']                                     AS allowance_key,
        CAST(NULL AS Nullable(String))                                       AS balance,
        CAST(NULL AS Nullable(String))                                       AS max_refill,
        CAST(NULL AS Nullable(String))                                       AS refill,
        CAST(NULL AS Nullable(String))                                       AS period,
        d.decoded_params['consumed']                                         AS consumed,
        d.decoded_params['newBalance']                                       AS new_balance,
        CAST(NULL AS Nullable(String))                                       AS default_role_key,
        d.block_timestamp,
        d.block_number,
        concat('0x', d.transaction_hash)                                     AS transaction_hash,
        d.log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'ConsumeAllowance') d
),

setup_and_default_rows AS (
    SELECT
        concat('0x', lower(contract_address))                                AS roles_module_address,
        event_name,
        lower(decoded_params['module'])                                      AS member_address,
        CAST(NULL AS Nullable(String))                                       AS role_key,
        CAST(NULL AS Nullable(UInt8))                                        AS is_member,
        CAST(NULL AS Nullable(String))                                       AS allowance_key,
        CAST(NULL AS Nullable(String))                                       AS balance,
        CAST(NULL AS Nullable(String))                                       AS max_refill,
        CAST(NULL AS Nullable(String))                                       AS refill,
        CAST(NULL AS Nullable(String))                                       AS period,
        CAST(NULL AS Nullable(String))                                       AS consumed,
        CAST(NULL AS Nullable(String))                                       AS new_balance,
        decoded_params['defaultRoleKey']                                     AS default_role_key,
        block_timestamp,
        block_number,
        concat('0x', transaction_hash)                                       AS transaction_hash,
        log_index
    FROM decoded
    WHERE event_name IN ('RolesModSetup','SetDefaultRole')
)

SELECT * FROM assign_role_rows
UNION ALL
SELECT * FROM set_allowance_rows
UNION ALL
SELECT * FROM consume_allowance_rows
UNION ALL
SELECT * FROM setup_and_default_rows
