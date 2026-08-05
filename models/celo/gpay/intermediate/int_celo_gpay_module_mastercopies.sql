{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='proxy_address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','identity']
  )
}}

{% set gp_start = '2026-01-01' %}
{% set factory   = '000000000000addb49795b0f9ba5bc298cdda236' %}  {# Zodiac ModuleProxyFactory (no 0x) #}
{% set roles_mc  = '732b9e9f259fba6f65a1a012dc89c20872ffbd2f' %}  {# GP patched Roles mastercopy, 2026-06-10 onwards (no 0x) #}
{% set roles_mc2 = '9646fdad06d3e24444381f44362a3b0eb343d337' %}  {# earlier Roles mastercopy, pilot cohort 2026-01..2026-06-12 (no 0x) #}
{% set delay_mc  = '22d903fd45f441f51bcad198d14eba8a75ea1ef0' %}  {# GP patched Delay mastercopy (no 0x) #}

-- GP Zodiac module proxies created via the canonical ModuleProxyFactory from the
-- Roles/Delay mastercopies GP provisions from. This is the DETERMINISTIC mastercopy
-- allowlist and exists ONLY as a cross-check for the bridge-fingerprint card
-- universe (int_celo_gpay_safe_registry) — it is NOT an inclusion source.
--
-- Two Roles mastercopies are in play, and both are needed for the cross-check to
-- cover the whole card base (verified on-chain 2026-08-03):
--   roles_mc  0x732b9e9f… — the current one, first use 2026-06-10, and the source
--             of every card since. Proxy count grows continuously with card
--             issuance; a handful are created-but-never-enabled.
--   roles_mc2 0x9646fdad… — CLOSED cohort: 587 proxies created 2026-01-15..
--             2026-06-12. 286 are GP Roles proxies, split across BOTH settlement
--             contracts: 235 wired AggregateBridge v1 0xc4df5cac… and 51 wired v2
--             0xc07cd8c2…. All 286 are now visible to int_celo_gpay_roles_modules,
--             which unions the seeded bridge set; before 2026-08-05 it keyed on v2
--             alone and the other 235 were an unmodelled card generation. The
--             remaining ~301 proxies belong to unrelated projects: this mastercopy
--             is SHARED, which is exactly why this model can never become an
--             inclusion source. The GP filter is the intersection with
--             int_celo_gpay_roles_modules, applied by every consumer.
-- With both included the allowlist covers 100% of the fingerprint Safes (drift probe
-- returned 0 rows on 2026-08-05, across both bridges); with only roles_mc it silently
-- missed the 51-card pilot cohort. Every GP Roles proxy so far
-- was created through this factory, so a fingerprint-only card now means a THIRD
-- mastercopy to add here. That drift check is a MANUAL probe (query in
-- models/celo/AGENTS.md), not a dbt test — a new mastercopy is legitimate GP activity
-- rather than a data defect. Run it before trusting a Celo card count.
--
-- Consequence of the shared mastercopy: a row count here is NOT a GP card count
-- (3491 rows on 2026-08-04, ~301 of them other projects' proxies). Anything that
-- aggregates this model must first intersect with int_celo_gpay_roles_modules.
--
-- Raw-slice ModuleProxyCreation(address indexed proxy, address indexed masterCopy)
-- => topic1 = proxy, topic2 = masterCopy. Bounded by card count; full rebuild.

SELECT
    concat('0x', substring(replaceAll(topic1, '0x', ''), 25, 40)) AS proxy_address,
    CASE substring(replaceAll(topic2, '0x', ''), 25, 40)
        WHEN '{{ roles_mc }}'  THEN 'roles_patched'
        WHEN '{{ roles_mc2 }}' THEN 'roles_pilot'
        WHEN '{{ delay_mc }}'  THEN 'delay_patched'
    END                                                           AS module_type,
    min(block_timestamp)                                          AS created_at
FROM {{ source('celo_execution', 'logs') }}
WHERE replaceAll(topic0, '0x', '') = '2150ada912bf189ed721c44211199e270903fc88008c2a1e1e889ef30fe67c5f'  -- ModuleProxyCreation
  AND block_timestamp >= toDateTime('{{ gp_start }}')
  AND lower(replaceAll(address, '0x', '')) = '{{ factory }}'
  AND substring(replaceAll(topic2, '0x', ''), 25, 40) IN ('{{ roles_mc }}', '{{ roles_mc2 }}', '{{ delay_mc }}')
GROUP BY proxy_address, module_type
