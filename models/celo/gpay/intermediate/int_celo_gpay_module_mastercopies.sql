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
{% set factory  = '000000000000addb49795b0f9ba5bc298cdda236' %}  {# Zodiac ModuleProxyFactory (no 0x) #}
{% set roles_mc = '732b9e9f259fba6f65a1a012dc89c20872ffbd2f' %}  {# GP patched Roles mastercopy (no 0x) #}
{% set delay_mc = '22d903fd45f441f51bcad198d14eba8a75ea1ef0' %}  {# GP patched Delay mastercopy (no 0x) #}

-- GP Zodiac module proxies created via the canonical ModuleProxyFactory from the
-- GP-patched Roles/Delay mastercopies. This is the DETERMINISTIC mastercopy
-- allowlist and exists ONLY as a cross-check for the bridge-fingerprint card
-- universe (int_celo_gpay_safe_registry) — it is NOT an inclusion source.
-- Verified on-chain: the mastercopy set (861 proxies) is a strict SUBSET of the
-- fingerprint universe (912 Safes), because GP also provisions some cards with a
-- Roles mastercopy outside this 2-address allowlist. Consumed by the identity
-- reconciliation test (assert every mastercopy-derived Safe is also found by the
-- fingerprint; a failure = the fingerprint developed a gap; a rising
-- fingerprint-only count = a new mastercopy to add here).
--
-- Raw-slice ModuleProxyCreation(address indexed proxy, address indexed masterCopy)
-- => topic1 = proxy, topic2 = masterCopy. Bounded by card count; full rebuild.

SELECT
    concat('0x', substring(replaceAll(topic1, '0x', ''), 25, 40)) AS proxy_address,
    CASE substring(replaceAll(topic2, '0x', ''), 25, 40)
        WHEN '{{ roles_mc }}' THEN 'roles_patched'
        WHEN '{{ delay_mc }}' THEN 'delay_patched'
    END                                                           AS module_type,
    min(block_timestamp)                                          AS created_at
FROM {{ source('celo_execution', 'logs') }}
WHERE replaceAll(topic0, '0x', '') = '2150ada912bf189ed721c44211199e270903fc88008c2a1e1e889ef30fe67c5f'  -- ModuleProxyCreation
  AND block_timestamp >= toDateTime('{{ gp_start }}')
  AND lower(replaceAll(address, '0x', '')) = '{{ factory }}'
  AND substring(replaceAll(topic2, '0x', ''), 25, 40) IN ('{{ roles_mc }}', '{{ delay_mc }}')
GROUP BY proxy_address, module_type
