{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_user_pseudonym, owner_user_pseudonym)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','safe','mixpanel']
  )
}}

-- Chain-wide Safe → owner pseudonym bridge.
-- One row per CURRENT (safe_address, owner) pair, projected into the
-- project-wide user_pseudonym hash space via `pseudonymize_address`.
--
-- ## Why this exists (cross-sector role)
-- A Safe is a smart-contract wallet — analytically, activity from a
-- Safe address belongs to its human owner(s), not to the Safe contract.
-- Without this bridge, any sector mart whose `user_pseudonym` is a Safe
-- (gpay's pay_wallets, gnosis_app account-abstraction wallets, validator
-- operators using a Safe withdrawal address, Circles avatars deployed
-- as Safes) is a dead-end in the user_pseudonym graph: it can only
-- overlap with other sectors AT the Safe-address pseudonym, missing the
-- shared-EOA identity that ties multiple Safes back to the same human.
--
-- This bridge fixes that. It exposes two `user_pseudonym`-typed columns:
--
--   - `safe_user_pseudonym`  — pseudonym of the Safe contract address
--   - `owner_user_pseudonym` — pseudonym of an EOA listed as a current
--                              owner of that Safe
--
-- Both hash into the SAME space as every other user-keyed mart (same
-- salt, same macro). So the planner can compose any safe-keyed sector
-- with any owner-keyed sector via two equi-joins:
--
--   sector_A.user_pseudonym = bridge.safe_user_pseudonym
--   bridge.owner_user_pseudonym = sector_B.user_pseudonym
--
-- See `semantic/relationships/user_pseudonym.yml` for the relationships
-- that surface this as a bridge to the planner.
--
-- ## Grain
-- `(safe_user_pseudonym, owner_user_pseudonym)` is the unique key. A
-- Safe with N current owners produces N rows. An EOA that owns M Safes
-- appears in M rows.
--
-- ## Column-side semantics (definitional, read this before sizing joins)
-- - `safe_user_pseudonym` is ONLY the Safe-contract side, by construction.
--   It will never contain an EOA pseudonym.
-- - `owner_user_pseudonym` is ONLY the owner side. It will be an EOA in
--   the common case, or another Safe pseudonym in the nested-Safe case
--   (a Safe owning another Safe).
-- An outer-join cardinality like "how many of mart X's pseudonyms appear
-- as safe_user_pseudonym here" is therefore NOT a coverage metric — it is
-- the fraction of X's pseudonyms that happen to be Safe contract addresses
-- (vs. EOAs). The relationships in `semantic/relationships/user_pseudonym
-- .yml` declare BOTH directions per mart so the planner reaches either
-- pseudonym flavour.
--
-- ## What's NOT here
-- - Historical ownership. We track only current owners; if you need
--   point-in-time fanout, join `int_execution_safes_owner_events`
--   directly (raw addresses, not pseudonymized).
-- - Delegate / module identities. Those are gpay-specific and live in
--   `int_execution_gpay_user_identity_bridge` (internal-only). This
--   bridge is the chain-wide projection for cross-sector composition
--   and intentionally restricts to the canonical OWNER role.
-- - Per-owner share / influence weighting. `current_threshold` is
--   denormalized onto every owner row so analysts can derive
--   1/threshold or 1/n_owners attribution downstream.

WITH per_safe AS (
    SELECT
        safe_address,
        count() AS n_owners
    FROM {{ ref('int_execution_safes_current_owners') }}
    GROUP BY safe_address
)

SELECT
    {{ pseudonymize_address('s.safe_address') }} AS safe_user_pseudonym,
    {{ pseudonymize_address('s.owner') }}        AS owner_user_pseudonym,
    s.became_owner_at                            AS became_owner_at,
    s.current_threshold                          AS current_threshold,
    p.n_owners                                   AS n_owners_for_safe
FROM {{ ref('int_execution_safes_current_owners') }} s
INNER JOIN per_safe p USING (safe_address)
WHERE s.owner IS NOT NULL
