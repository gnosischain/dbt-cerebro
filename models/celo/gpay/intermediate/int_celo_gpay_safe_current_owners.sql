{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, owner_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','owner_events']
  )
}}

-- Current (as of now) owner set per Safe, folded directly from the
-- unified crawlers_data.celo_gpay_wallet_events log. Mirrors Gnosis
-- Chain's int_execution_safes_current_owners exactly: for each
-- (safe_address, owner_address) pair, take whichever event happened
-- last; keep the pair only if that last event was issued_at or add_owner.
--
-- No synthetic seed row needed here (unlike the earlier version of this
-- model) — issued_at is now a real ingested event, not something derived
-- separately in a different model, so a Safe with no add/remove events at
-- all still naturally has its initial owner represented in this same
-- source.
--
-- Tie-break: GP's Celo provisioning commonly swaps the owner within
-- seconds of SafeSetup (sometimes, though not always, the exact same
-- timestamp — see sources.yml for what was observed), so issued_at and a
-- same-second remove_owner are possible. is_mutation (0 for issued_at, 1
-- for add_owner/remove_owner) breaks a tie in favor of the mutation event
-- winning argMax's tuple comparison — issuance must, causally, precede
-- any subsequent owner change for the same Safe, even on the rare
-- occasion they share an identical event_time.

WITH events AS (
    SELECT
        safe_address,
        action,
        action_value AS owner_address,
        event_time,
        CASE WHEN action = 'issued_at' THEN 0 ELSE 1 END AS is_mutation
    FROM {{ ref('int_celo_gpay_wallet_events') }}
    WHERE owner_address IS NOT NULL
),

agg AS (
    SELECT
        safe_address,
        owner_address,
        argMax(action,     (event_time, is_mutation)) AS last_action,
        argMax(event_time, (event_time, is_mutation)) AS became_owner_at
    FROM events
    GROUP BY safe_address, owner_address
)

SELECT
    safe_address,
    owner_address,
    became_owner_at
FROM agg
WHERE last_action IN ('issued_at', 'add_owner')
