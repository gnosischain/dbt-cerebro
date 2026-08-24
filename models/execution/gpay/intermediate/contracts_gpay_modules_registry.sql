{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','registry']
  )
}}

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet
    FROM {{ ref('int_execution_gpay_wallets') }}
),

-- Every (module_proxy, first_enabled_at) pair that was enabled on a GP Safe.
enabled_on_gp AS (
    SELECT
        lower(target_address) AS module_proxy,
        min(block_timestamp)  AS first_enabled_at
    FROM {{ ref('int_execution_safes_module_events') }}
    WHERE event_kind = 'enabled_module'
      AND lower(safe_address) IN (SELECT pay_wallet FROM gpay_safes)
      AND target_address IS NOT NULL
    GROUP BY module_proxy
),

-- Same proxies, joined to the Zodiac factory discovery so we know
-- which mastercopy each one points at, and therefore the contract type.
typed AS (
    SELECT
        e.module_proxy                                                 AS address,
        multiIf(
            p.master_copy = '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5', 'DelayModule',
            p.master_copy = '0x9646fdad06d3e24444381f44362a3b0eb343d337', 'RolesModule',
            p.master_copy = '0x732b9e9f259fba6f65a1a012dc89c20872ffbd2f', 'RolesModule',
            'Unknown'
        )                                                              AS contract_type,
        p.master_copy                                                  AS abi_source_address,
        toUInt8(1)                                                     AS is_dynamic,
        e.first_enabled_at                                             AS start_blocktime,
        'gpay_module_enabled_x_proxy_factory'                          AS discovery_source
    FROM enabled_on_gp e
    INNER JOIN {{ ref('int_execution_zodiac_module_proxies') }} p
        ON p.proxy_address = e.module_proxy
    -- 0x732b9e9f... = post-June-2026-migration Zodiac Roles mastercopy (71,425 new card proxies), so the
    -- allowances / spender-delegate models see migrated cards. The migration's new DELAY mastercopy
    -- 0x22d903fd... is intentionally NOT registered: registering it would give migrated safes a real
    -- DelayModule row, trip the canonical-inheritance guard in int_execution_gpay_safe_modules, and
    -- re-break GA top-up attribution. Migrated safes keep the OLD safe's DelayModule (inherited) for
    -- GA-ownership continuity across the migration.
    WHERE p.master_copy IN (
        '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5',
        '0x9646fdad06d3e24444381f44362a3b0eb343d337',
        '0x732b9e9f259fba6f65a1a012dc89c20872ffbd2f'
    )
),

-- The Spender is a SINGLETON, not a per-card module, so it can NEVER arrive through
-- `enabled_on_gp` above: that CTE only admits proxies enabled on an address in
-- int_execution_gpay_wallets, and the Spender is enabled on Gnosis Pay's own infra
-- safes -- 0x896a695d5ccdd21f9e3bb18307a558befccb8428 for the two live proxies,
-- 0xab270c7549a5662ce96580dcd9eb3a96046a1945 for the 2023 one. Verified: all three
-- proxies have module events, ZERO of them on a card safe. The per-card filter is
-- structurally incapable of seeing it, which is why the SpenderModule branch of the
-- multiIf above was dead code and int_execution_gpay_spender_events held 0 rows from
-- 2023-12 until this fix, taking fct_mixpanel_ga_gpay_users.spends_last_30d to 0 with it.
--
-- So register the singletons straight from factory discovery instead, keyed on the
-- mastercopy rather than on who enabled them:
--   0x70db5361... deployed 2023-12, two proxies (0x67219ab4..., 0xcff260bf...)
--   0x7a592bae... deployed 2026-06-04, one proxy (0x5f07734e...) -- the live one,
--                 emitting ~40.8k Spend events per week as of 2026-08-24
-- Both mastercopies carry a byte-identical ABI (sha256 3c5c6f3dff516d2b, 7,238 bytes),
-- so both resolve against the same 11 seeded event signatures.
--
-- This adds NO rows to int_execution_gpay_safe_modules: that model starts from module
-- events on GP card safes and INNER JOINs this registry, so a module no card safe ever
-- enabled cannot reach it. That is the guard the Delay-mastercopy note above is about --
-- it holds here for the opposite reason (never enabled on a card safe at all), not by
-- deliberate omission.
spender_singletons AS (
    SELECT
        lower(p.proxy_address)                          AS address,
        CAST('SpenderModule' AS String)                 AS contract_type,
        lower(p.master_copy)                            AS abi_source_address,
        toUInt8(1)                                      AS is_dynamic,
        p.block_timestamp                               AS start_blocktime,
        CAST('zodiac_proxy_factory_spender_singleton' AS String) AS discovery_source
    FROM {{ ref('int_execution_zodiac_module_proxies') }} p
    WHERE lower(p.master_copy) IN (
        '0x70db53617d170a4e407e00dff718099539134f9a',
        '0x7a592bae57b8cd45688f9eb81ce4a622e7e37cb7'
    )
)

-- typed.address comes from int_execution_safes_module_events.target_address,
-- which is already 0x-prefixed (decode_logs writes address-typed decoded_params
-- with the prefix). Same for typed.abi_source_address from
-- int_execution_zodiac_module_proxies.master_copy. No re-prefixing.
SELECT
    address,
    contract_type,
    abi_source_address,
    is_dynamic,
    start_blocktime,
    discovery_source
FROM typed

UNION ALL

SELECT
    address,
    contract_type,
    abi_source_address,
    is_dynamic,
    start_blocktime,
    discovery_source
FROM spender_singletons
