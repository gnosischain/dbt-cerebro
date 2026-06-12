

-- INTERNAL ONLY — GP-side analogue of int_execution_gnosis_app_user_identity_bridge.
-- Holds raw GP addresses (Safe contracts, owners, delegates) paired with their
-- user_pseudonym and identity_role. Used by GP-side downstream models to
-- pseudonymize raw addresses at build time without recomputing the hash.
-- Never exposed to MCP or API.
--
-- IMPORTANT — `gp_safe` MUST be in the ReplacingMergeTree order_by. Gnosis
-- Pay uses a single shared delegate-signer EOA across all Safes, so for the
-- `delegate` role the (user_pseudonym, identity_role, address) tuple is
-- identical for every (delegate, Safe) pair (~30k rows on 2026-05). Without
-- gp_safe in the dedup key, ReplacingMergeTree collapses all delegate
-- relationships down to a single row, silently losing the delegate↔Safe
-- mapping that downstream consumers (e.g. int_execution_gpay_conversions,
-- int_execution_gpay_user_events_unified, and the
-- int_execution_gpay_safe_identities projection) depend on.
--
-- int_execution_gpay_safe_identities is a 3-column projection of this model
-- (drops address + timestamps) and is the safe-to-expose mart-facing
-- pseudonymization boundary.

WITH gp_safes AS (
    SELECT lower(address) AS gp_safe FROM `dbt`.`int_execution_gpay_wallets`
),

initial_owners AS (
    SELECT
        lower(oe.owner)                              AS address,
        
    sipHash64(concat(unhex('00'), lower(oe.owner)))
       AS user_pseudonym,
        'initial_owner'                              AS identity_role,
        oe.safe_address                              AS gp_safe,
        toDateTime64(oe.block_timestamp, 0, 'UTC')   AS first_seen_at,
        toDateTime64(oe.block_timestamp, 0, 'UTC')   AS last_seen_at
    FROM `dbt`.`int_execution_safes_owner_events` oe
    INNER JOIN gp_safes gs ON lower(oe.safe_address) = gs.gp_safe
    WHERE oe.event_kind = 'safe_setup'
      AND oe.owner IS NOT NULL
),

delegates AS (
    SELECT
        lower(d.delegate_address)                            AS address,
        
    sipHash64(concat(unhex('00'), lower(d.delegate_address)))
     AS user_pseudonym,
        'delegate'                                           AS identity_role,
        d.gp_safe                                            AS gp_safe,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC')))         AS first_seen_at,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC')))         AS last_seen_at
    FROM `dbt`.`int_execution_gpay_spender_delegates_current` d
),

-- June 2026 Safe migration: an OLD Safe's pseudonym is keyed on its
-- canonical (NEW) Safe so the same user does not split into two
-- pseudonyms across the migration. The migrated_links rows make the
-- old->new relationship queryable directly.
safe_self AS (
    SELECT
        gs.gp_safe                              AS address,
        
    sipHash64(concat(unhex('00'), lower(if(c.canonical_address != '', c.canonical_address, gs.gp_safe))))
 AS user_pseudonym,
        'safe_self'                             AS identity_role,
        gs.gp_safe                              AS gp_safe,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC'))) AS first_seen_at,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC'))) AS last_seen_at
    FROM gp_safes gs
    LEFT JOIN `dbt`.`int_execution_gpay_safe_canonical` c
        ON gs.gp_safe = c.address
),

migrated_links AS (
    SELECT
        c.address                                          AS address,
        
    sipHash64(concat(unhex('00'), lower(c.canonical_address)))
  AS user_pseudonym,
        'migrated_old_safe'                                AS identity_role,
        c.canonical_address                                AS gp_safe,
        CAST(toDateTime64(c.migrated_at, 0, 'UTC') AS Nullable(DateTime64(0, 'UTC'))) AS first_seen_at,
        CAST(toDateTime64(c.migrated_at, 0, 'UTC') AS Nullable(DateTime64(0, 'UTC'))) AS last_seen_at
    FROM `dbt`.`int_execution_gpay_safe_canonical` c
)

SELECT * FROM initial_owners
UNION ALL
SELECT * FROM delegates
UNION ALL
SELECT * FROM safe_self
UNION ALL
SELECT * FROM migrated_links