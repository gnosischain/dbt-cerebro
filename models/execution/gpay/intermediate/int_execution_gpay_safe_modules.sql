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
)

-- m.safe_address and m.module_proxy are both already 0x-prefixed
-- (inherited from int_execution_safes_module_events, which uses
-- decode_logs-decoded address columns). r.address from the registry is
-- also already 0x-prefixed (after the registry's own re-prefixing fix).
-- No concat needed on either side.
SELECT
    m.safe_address                          AS gp_safe,
    r.contract_type                         AS contract_type,
    r.address                               AS module_proxy_address,
    m.last_event_time                       AS enabled_at
FROM module_state_latest m
INNER JOIN {{ ref('contracts_gpay_modules_registry') }} r
    ON r.address = m.module_proxy
WHERE m.last_event_kind = 'enabled_module'
