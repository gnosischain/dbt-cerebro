{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','mixpanel']
  )
}}

-- Per-user-pseudonym projection of int_execution_gnosis_app_user_identities
-- enriched with one boolean column per identification heuristic. Source is
-- ALREADY 1 row per user_pseudonym (the intermediate is the
-- pseudonymization boundary for the Gnosis App sector), so this view just
-- pivots the `heuristic_kinds` array into named flags + computes a
-- high-confidence indicator.
--
-- ## Cross-sector role
-- `user_pseudonym` is the canonical cross-domain join key produced by
-- `pseudonymize_address(addr)` — SAME hash space as:
--   * revenue per-user marts (fct_revenue_per_user_*)
--   * gpay user identities (fct_execution_gpay_users_distinct)
--   * Mixpanel `user_id_hash` (stg_mixpanel_ga__events)
--
-- This means analysts can compute three-way overlaps directly via the
-- semantic layer's user_pseudonym entity graph:
--   - revenue-active users who also use the Gnosis App on-chain
--   - Gnosis App on-chain users who hold a Gnosis Pay Safe
--   - Mixpanel-identified users who appear in any of the on-chain sectors
--
-- ## "Gnosis App" usage notes
-- "Gnosis App" refers to the consumer app at app.gnosis.io. It has TWO
-- complementary data domains:
--   1. Web/mobile analytics    — sourced from Mixpanel (module:
--                                mixpanel_ga). DAU, page views, feature
--                                engagement, modals, funnels.
--   2. ON-CHAIN behaviour      — sourced from execution / contract
--                                events (this module). Address-level
--                                activity that the heuristics here
--                                identify as belonging to a Gnosis App
--                                user.
-- Analyst-discovery queries for "Gnosis App" should surface metrics
-- from BOTH domains. Synonyms on the semantic_model below intentionally
-- include terms used by the Mixpanel side so cross-domain queries fan
-- out correctly.

SELECT
    user_pseudonym,
    first_seen_at,
    last_seen_at,
    n_distinct_heuristics,

    -- Per-heuristic flags. Lets analysts answer "how many Gnosis App
    -- users came from Circles trust graph vs invite flow vs Safe
    -- invitation module" without re-pivoting the array.
    has(heuristic_kinds, 'circles_trust')           AS via_circles_trust,
    has(heuristic_kinds, 'circles_fee')             AS via_circles_fee,
    has(heuristic_kinds, 'circles_profile_update')  AS via_circles_profile_update,
    has(heuristic_kinds, 'circles_invite_human')    AS via_circles_invite_human,
    has(heuristic_kinds, 'circles_register_human')  AS via_circles_register_human,
    has(heuristic_kinds, 'circles_personal_mint')   AS via_circles_personal_mint,
    has(heuristic_kinds, 'safe_invitation_module')  AS via_safe_invitation_module,

    -- Confidence proxy: ≥2 independent heuristics fired for this user.
    -- Anything with n_distinct_heuristics=1 could in principle be a
    -- single-event false positive; downstream marts typically restrict
    -- to high-confidence users when measuring true Gnosis App reach.
    (n_distinct_heuristics >= 2)                    AS is_high_confidence
FROM {{ ref('int_execution_gnosis_app_user_identities') }}
WHERE user_pseudonym IS NOT NULL
