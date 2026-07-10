{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, action, action_value, event_time)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','wallet_events']
  )
}}

-- Thin pass-through of the unified, append-only wallet-lifecycle event log
-- (issued_at + add_owner + remove_owner). Bounded by card count, not
-- transaction volume, so a full rebuild stays cheap indefinitely — no
-- incremental treatment needed here, unlike int_celo_gpay_activity.
-- Consumed by int_celo_gpay_wallets.sql (issued_at only) and
-- int_celo_gpay_safe_current_owners.sql (the full fold).

SELECT
    safe_address,
    action,
    action_value,
    event_time
FROM {{ source('crawlers_data', 'celo_gpay_wallet_events') }}
FINAL
