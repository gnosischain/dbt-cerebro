

-- Week-bucketed Circles v2 activity, one row per (week, address) where
-- the address is the avatar that touched any of the canonical "alive"
-- on-chain signals:
--
--   * Registered as a Human   → int_execution_circles_v2_avatars (avatar)
--   * Trusted someone         → int_execution_circles_v2_trust_updates (truster)
--   * Personal-minted CRC     → int_execution_circles_v2_hub_transfers (to_address, from_address=0x00)
--
-- Mirrors the `weekly_active_avatars_circles` CTE from the Dune
-- circles-v2-kpis dashboard. The Dune query additionally unions
-- NameRegistry.UpdateMetadataDigest and Hub.StreamCompleted; those decoded
-- event streams are not yet exposed in dbt-cerebro so we accept the small
-- under-count and document the deviation here. When those staging models
-- land, add `stream_completed` and `name_registry_metadata` source CTEs to
-- the UNION below.
--
-- Materialised as a full-rebuild table (small post-aggregation) so the
-- downstream blacklist join always re-evaluates against the latest
-- crawlers_data.circles_blacklisted snapshot.

WITH register_human AS (
    SELECT
        toStartOfWeek(block_timestamp, 1) AS week,
        avatar                            AS address
    FROM `dbt`.`int_execution_circles_v2_avatars`
    WHERE avatar_type = 'Human'
      AND block_timestamp < today()
),

trusts AS (
    SELECT
        toStartOfWeek(block_timestamp, 1) AS week,
        truster                           AS address
    FROM `dbt`.`int_execution_circles_v2_trust_updates`
    WHERE block_timestamp < today()
),

mints AS (
    -- Personal mints only: "active minters" should not be inflated by
    -- group mints (where the recipient is a depositor, not the minter)
    -- or by V1→V2 migrations.
    SELECT
        toStartOfWeek(block_timestamp, 1) AS week,
        to_address                        AS address
    FROM `dbt`.`int_execution_circles_v2_mint_events`
    WHERE mint_kind = 'personal'
      AND block_timestamp < today()
)

SELECT DISTINCT week, address
FROM (
    SELECT week, address FROM register_human
    UNION ALL
    SELECT week, address FROM trusts
    UNION ALL
    SELECT week, address FROM mints
)
WHERE address != ''