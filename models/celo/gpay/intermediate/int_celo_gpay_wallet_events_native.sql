{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, action, action_value, event_time)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','wallet_events']
  )
}}

-- Native twin of crawlers_data.celo_gpay_wallet_events, derived entirely
-- from celo_execution instead of the Dune spine. Same shape and action
-- vocabulary ('issued_at' / 'add_owner' / 'remove_owner') so downstream
-- folds (int_celo_gpay_wallets, int_celo_gpay_safe_current_owners) can be
-- repointed with a one-line ref swap once reconciliation signs off.
--
-- issued_at = the SafeSetup owner(s) of a Safe that is in the GP registry
-- (int_celo_gpay_safe_registry gates membership; SafeSetup alone would
-- capture every Safe on Celo). Event time is the SafeSetup block, matching
-- the Dune spine's issuance timestamp semantics.

WITH registry AS (
    SELECT address FROM {{ ref('int_celo_gpay_safe_registry') }}
)

SELECT
    e.safe_address,
    CASE e.event_kind
        WHEN 'safe_setup'    THEN 'issued_at'
        WHEN 'added_owner'   THEN 'add_owner'
        WHEN 'removed_owner' THEN 'remove_owner'
    END                 AS action,
    e.owner             AS action_value,
    e.block_timestamp   AS event_time
FROM {{ ref('int_celo_gpay_safe_events_native') }} e
WHERE e.safe_address IN (SELECT address FROM registry)
