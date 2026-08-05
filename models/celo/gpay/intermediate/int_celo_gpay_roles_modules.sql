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

-- GP-specific Zodiac Roles module proxies on Celo, discovered natively from
-- celo_execution.logs (replaces the roles_with_bridge CTE of Dune query
-- 7808895). Only GP card Safes wire a GP AggregateBridge into their Roles
-- module as an authorized submodule, so an EnabledModule(bridge) event emitted
-- BY a Roles proxy is a GP-specific fingerprint, set at card provisioning
-- (before any spend).
--
-- THE FINGERPRINT IS A SET, NOT ONE ADDRESS. Two settlement contracts are live
-- at once: v1 0xc4df5cac… (original, from 2026-03-31) and v2 0xc07cd8c2… (from
-- 2026-05-28). Gnosis Pay confirmed on 2026-08-05 that v1 is theirs and will be
-- migrated onto v2. Keying discovery on v2 alone hid 235 real cards and ~34% of
-- all settlement traffic for months, and made March–May look empty when the
-- product was already running (docs/lessons/circular-completeness-proof.md).
-- The set is seeded, never hardcoded — celo_gpay_settlement_contracts — so the
-- migration target is a one-line seed edit rather than a change in three models.
--
-- MIGRATION SAFETY: a card that migrates emits a SECOND EnabledModule for the new
-- bridge from the SAME Roles proxy. GROUP BY address collapses that to one row and
-- min(block_timestamp) keeps the ORIGINAL provisioning time, so migration never
-- duplicates a card nor resets its age. wired_settlements exposes the transition —
-- a module listing two addresses has migrated. Zero modules wire both as of
-- 2026-08-05, i.e. the migration has not started.
-- Do NOT turn "which bridge" into a per-card column: it is a property of each
-- settlement TRANSFER (see int_celo_gpay_activity.settlement_address), and a card
-- attribute would silently go stale the day that card moves.
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
    min(block_timestamp)          AS first_seen_at,
    arraySort(groupUniqArray(
        concat('0x', substring(replaceAll(data, '0x', ''), 25, 40))
    ))                            AS wired_settlements
FROM {{ source('celo_execution', 'logs') }}
WHERE replaceAll(topic0, '0x', '') = 'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440'  -- EnabledModule
  AND block_timestamp >= toDateTime('{{ gp_start }}')
  AND substring(replaceAll(data, '0x', ''), 25, 40) IN (
      SELECT lower(replaceAll(address, '0x', ''))
      FROM {{ ref('celo_gpay_settlement_contracts') }}
      WHERE status IN ('active', 'migrating')
  )
GROUP BY address
