

-- True Gnosis Pay account universe, identified by the DEPLOY-TIME on-chain
-- signature: any Safe that enabled a Gnosis Pay Zodiac module (Delay / Roles /
-- Spender), independent of whether it ever made a card payment. This deliberately
-- does NOT go through int_execution_gpay_wallets (which is payment-gated at the
-- root), so it also counts accounts that were deployed but never spent -- the
-- correct basis for "accounts deployed" versus the funded/first-payment proxy.

WITH gp_module_proxies AS (
    SELECT lower(proxy_address) AS module_proxy
    FROM `dbt`.`int_execution_zodiac_module_proxies`
    WHERE master_copy IN (
        '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5',  -- DelayModule
        '0x9646fdad06d3e24444381f44362a3b0eb343d337',  -- RolesModule
        '0x732b9e9f259fba6f65a1a012dc89c20872ffbd2f',  -- RolesModule (post-June-2026 migration)
        '0x70db53617d170a4e407e00dff718099539134f9a'   -- SpenderModule
    )
),

-- One row per GP-module-enable event, mapped to its account. The June 2026
-- post-exploit migration is collapsed here (old -> canonical NEW safe) so a
-- migrated user counts once; join_use_nulls=1 makes the unmatched right side
-- NULL so coalesce falls back to the safe's own address. No aggregation yet,
-- so the single min() below stays a one-level aggregate over raw timestamps.
account_events AS (
    SELECT
        coalesce(c.canonical_address, lower(m.safe_address)) AS account,
        m.block_timestamp                                    AS event_time
    FROM `dbt`.`int_execution_safes_module_events` m
    LEFT JOIN `dbt`.`int_execution_gpay_safe_canonical` c
        ON c.address = lower(m.safe_address)
    WHERE m.event_kind = 'enabled_module'
      AND lower(m.target_address) IN (SELECT module_proxy FROM gp_module_proxies)
)

SELECT
    account,
    min(event_time)         AS deployed_at,
    toDate(min(event_time)) AS deployed_date
FROM account_events
GROUP BY account