{{ config(tags=['celo','gpay','identity','reconciliation']) }}

-- Identity reconciliation guard for the Celo GP card universe.
--
-- The card universe (int_celo_gpay_safe_registry) is built from the bridge
-- FINGERPRINT (Safes that enabled a bridge-granting Roles proxy) — verified the
-- authoritative, complete signal (912 cards). The mastercopy allowlist
-- (int_celo_gpay_module_mastercopies) is an INDEPENDENT, deterministic method
-- and was verified on-chain to be a strict SUBSET (mc ⊆ fp).
--
-- This test asserts that invariant still holds: every Safe discovered via the
-- mastercopy allowlist MUST also be in the fingerprint registry. A failure means
-- the fingerprint developed a gap (a real card the fingerprint no longer sees) —
-- investigate before trusting the numbers. (The reverse direction, fingerprint-
-- only cards, is EXPECTED and healthy — GP uses Roles mastercopies beyond the
-- 2-address allowlist — so it is intentionally NOT asserted here; monitor it
-- separately if you want a mastercopy-drift signal.)
--
-- Returns offending rows => dbt test fails.

{% set gp_start = '2026-01-01' %}  {# GP era floor #}

WITH mc_proxies AS (
    SELECT lower(replaceAll(proxy_address, '0x', '')) AS proxy
    FROM {{ ref('int_celo_gpay_module_mastercopies') }}
),

mc_safes AS (
    SELECT DISTINCT concat('0x', lower(address)) AS safe_address
    FROM {{ source('celo_execution', 'logs') }}
    WHERE replaceAll(topic0, '0x', '') = 'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440'  -- EnabledModule
      AND block_timestamp >= toDateTime('{{ gp_start }}')
      AND substring(replaceAll(data, '0x', ''), 25, 40) IN (SELECT proxy FROM mc_proxies)
)

SELECT m.safe_address
FROM mc_safes m
LEFT JOIN {{ ref('int_celo_gpay_safe_registry') }} r
    ON r.address = m.safe_address
WHERE r.address IS NULL
