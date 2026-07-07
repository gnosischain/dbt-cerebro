{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(gp_safe, contract_type)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay']
  )
}}

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet FROM {{ ref('int_execution_gpay_wallets') }}
),

module_state_latest AS (
    SELECT
        lower(safe_address)  AS safe_address,
        lower(target_address) AS module_proxy,
        argMax(event_kind,      (block_number, log_index)) AS last_event_kind,
        argMax(block_timestamp, (block_number, log_index)) AS last_event_time
    FROM {{ ref('int_execution_safes_module_events') }}
    WHERE event_kind IN ('enabled_module','disabled_module')
      AND lower(safe_address) IN (SELECT pay_wallet FROM gpay_safes)
      AND target_address IS NOT NULL
    GROUP BY safe_address, module_proxy
),

-- m.safe_address and m.module_proxy are both already 0x-prefixed
-- (inherited from int_execution_safes_module_events, which uses
-- decode_logs-decoded address columns). r.address from the registry is
-- also already 0x-prefixed (after the registry's own re-prefixing fix).
-- No concat needed on either side.
base_modules AS (
    SELECT
        m.safe_address                          AS gp_safe,
        r.contract_type                         AS contract_type,
        r.address                               AS module_proxy_address,
        m.last_event_time                       AS enabled_at
    FROM module_state_latest m
    INNER JOIN {{ ref('contracts_gpay_modules_registry') }} r
        ON r.address = m.module_proxy
    WHERE m.last_event_kind = 'enabled_module'
)

SELECT gp_safe, contract_type, module_proxy_address, enabled_at
FROM base_modules

UNION ALL

-- June-2026 Safe migration: canonical NEW safes were redeployed with a new Zodiac mastercopy
-- that is not in contracts_gpay_modules_registry, so they get no module row here and read as
-- not-GA-owned downstream (top-ups collapsed 9,315 -> 149; card KPI froze at 1,185). The new
-- safe's own delay module never re-enabled the GA user, so carry the OLD safe's DelayModule
-- onto the migrated NEW safe to preserve the pre-migration GA-ownership across the migration.
-- Guarded to migrated safes that don't already have their own DelayModule row.
SELECT
    lower(c.canonical_address)              AS gp_safe,
    b.contract_type                         AS contract_type,
    b.module_proxy_address                  AS module_proxy_address,
    b.enabled_at                            AS enabled_at
FROM base_modules b
INNER JOIN {{ ref('int_execution_gpay_safe_canonical') }} c
    ON lower(c.address) = lower(b.gp_safe)
WHERE b.contract_type = 'DelayModule'
  AND lower(c.canonical_address) NOT IN (
      SELECT lower(gp_safe) FROM base_modules WHERE contract_type = 'DelayModule'
  )
