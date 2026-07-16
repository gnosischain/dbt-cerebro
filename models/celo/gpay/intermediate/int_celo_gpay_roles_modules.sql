{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','registry']
  )
}}

-- GP-specific Zodiac Roles module proxies on Celo, discovered natively from
-- celo_execution.logs (replaces the roles_with_bridge CTE of Dune query
-- 7808895). Only GP card Safes wire the GP AggregateBridge into their Roles
-- module as an authorized submodule, so an EnabledModule(bridge) event emitted
-- BY a Roles proxy is a GP-specific fingerprint, set at card provisioning
-- (before any spend).
--
-- Raw topic slicing instead of the decode macro: EnabledModule's single
-- `module` param is non-indexed (data bytes 13-32), the layout is fixed, and
-- this registry is itself an input to the decoded layer — slicing avoids a
-- registry-needs-decoding-needs-registry loop.
--
-- Full rebuild every run (bounded by card count, not tx volume). The date
-- floor matches the Dune spine (2026-01-01, comfortably before the June 2026
-- launch) and prunes the logs scan to the GP era.

SELECT
    concat('0x', lower(address))  AS address,
    'RolesModProxy'               AS contract_type,
    min(block_timestamp)          AS first_seen_at
FROM {{ source('celo_execution', 'logs') }}
WHERE replaceAll(topic0, '0x', '') = 'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440'  -- EnabledModule
  AND block_timestamp >= toDateTime('2026-01-01')
  AND substring(replaceAll(data, '0x', ''), 25, 40) = 'c07cd8c24fb384d5e2b60a3ef39751f5d4cb69e1'          -- AggregateBridge
GROUP BY address
