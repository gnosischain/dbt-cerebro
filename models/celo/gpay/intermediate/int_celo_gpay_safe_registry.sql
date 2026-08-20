{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','registry']
  )
}}

{% set gp_start = '2026-01-01' %}  {# GP era floor #}

-- GP card Safes on Celo, discovered natively (replaces the fp CTE of Dune
-- query 7808895): a Safe that emits EnabledModule(<GP Roles proxy>) has been
-- provisioned as a GP card. Confirmed GP pre-spend, so this registry also
-- covers created-but-never-funded cards for the funnel top.
--
-- Population = the UNION over every in-scope settlement contract, inherited from
-- int_celo_gpay_roles_modules (seeded in celo_gpay_settlement_contracts). Both the
-- legacy bridge scheduled for migration and the current bridge count: they are one
-- card program, not two, and 235 real cards sat outside this registry while it was
-- keyed on the current bridge alone. See docs/lessons/circular-completeness-proof.md.
--
-- EVERY settlement contract is excluded, not just one (settlement sinks, not user
-- cards — mirrors Dune exclusion list query 7809356). A bridge enables modules
-- too, so leaving one out of this filter would enrol the bridge itself as a card.
--
-- `address` + `contract_type` columns follow the whitelist shape the decode
-- macros expect via contract_address_ref (see int_celo_gpay_safe_events_native).
-- Full rebuild every run; bounded by card count.

SELECT
    concat('0x', lower(l.address))  AS address,
    'SafeProxy'                     AS contract_type,
    min(l.block_timestamp)          AS module_enabled_at
FROM {{ source('celo_execution', 'logs') }} l
WHERE replaceAll(l.topic0, '0x', '') = 'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440'  -- EnabledModule
  AND l.block_timestamp >= toDateTime('{{ gp_start }}')
  AND substring(replaceAll(l.data, '0x', ''), 25, 40) IN (
      SELECT lower(replaceAll(address, '0x', ''))
      FROM {{ ref('int_celo_gpay_roles_modules') }}
  )
  AND lower(l.address) NOT IN (
      SELECT lower(replaceAll(address, '0x', ''))
      FROM {{ ref('celo_gpay_settlement_contracts') }}
  )
GROUP BY l.address
