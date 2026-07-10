



-- Reconstructs the wallet list from the append-only
-- crawlers_data.celo_gpay_wallet_events log (action='issued_at') rather
-- than reading a per-safe snapshot table directly — the earlier
-- celo_gpay_wallets table stored is_activated/first_spend_at/owner_address
-- as MUTABLE per-safe fields (same safe_address reinserted daily with
-- evolving values), which is exactly the class of source that breaks
-- incremental models downstream. Event-sourcing issued_at removes that
-- entirely: each issued_at row is an immutable fact, written once, never
-- revised.
--
-- first_spend_at/is_activated are derived directly from
-- crawlers_data.celo_gpay_transfers here, not stored as an ingested event
-- at all — the fact is already fully present in the transfers data
-- (anything sent to the settlement address, by the same definition the
-- Dune spine itself uses for its own activation signal), so ingesting it
-- separately would just be storing the same fact twice. This reads
-- directly from the source table (not through int_celo_gpay_activity, or
-- through this model itself) specifically to avoid a circular dbt ref:
-- int_celo_gpay_activity depends on this model for wallet-membership
-- classification, so this model computing first_spend_at via
-- int_celo_gpay_activity would create a dependency cycle. Reading the
-- unfiltered source directly (no wallet-membership check needed, since
-- anything sent to the settlement address is presumed to be a GP Safe,
-- matching the spine's own definition) avoids that entirely. This is a
-- full-table GROUP BY over the whole transfers history on every rebuild —
-- confirmed acceptable at scale by precedent: Gnosis Chain's own
-- int_execution_gpay_wallets does an equivalent full scan of the much
-- larger execution.logs on every run, materialized='table', successfully.

WITH issued AS (
    SELECT
        safe_address,
        min(event_time)          AS issued_at_ts,
        any(action_value)        AS owner_address,
        groupArray(action_value) AS owners
    FROM `dbt`.`int_celo_gpay_wallet_events`
    WHERE action = 'issued_at'
    GROUP BY safe_address
),

activation AS (
    SELECT
        sender          AS safe_address,
        min(block_date) AS first_spend_at
    FROM `crawlers_data`.`celo_gpay_transfers`
    FINAL
    WHERE receiver = '0xc07cd8c24fb384d5e2b60a3ef39751f5d4cb69e1'
    GROUP BY sender
)

SELECT
    i.safe_address,
    i.owner_address,
    i.owners,
    toDate(i.issued_at_ts)                        AS issued_at,
    -- block_date is a non-nullable Date, so an unmatched LEFT JOIN fills
    -- a.first_spend_at with ClickHouse's type default (1970-01-01), not
    -- NULL — nullIf converts that sentinel back into a real NULL. Without
    -- this, every unactivated wallet reads as "activated on day zero".
    nullIf(a.first_spend_at, toDate(0))           AS first_spend_at,
    nullIf(a.first_spend_at, toDate(0)) IS NOT NULL AS is_activated
FROM issued i
LEFT JOIN activation a ON a.safe_address = i.safe_address