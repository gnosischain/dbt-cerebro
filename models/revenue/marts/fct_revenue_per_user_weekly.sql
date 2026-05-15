{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross']
  )
}}

-- Per-user weekly revenue aggregation — collapses
-- int_revenue_fees_weekly_per_user (1 row per stream/symbol/user/week)
-- down to (week, user_pseudonym, rolling_fees_total, ...).
--
-- Privacy boundary: the raw `user` (wallet address) column is hashed
-- to user_pseudonym via the pseudonymize_address macro and dropped.
-- user_pseudonym is the canonical cross-domain join key — same hash
-- space as gpay safe identities, mixpanel user_id_hash, and gnosis
-- app user_pseudonym.
--
-- Used by the semantic layer as the primary user-keyed revenue entry
-- point. Join to gpay / gnosis_app / mixpanel user-keyed semantic_models
-- on `user_pseudonym = user_pseudonym` to compute cross-sector overlaps
-- (e.g. "revenue-active users who are also gpay users this week").

SELECT
    week,
    {{ pseudonymize_address('user') }}                 AS user_pseudonym,
    round(sum(annual_rolling_fees), 2)                  AS rolling_fees_total,
    -- Sector presence flags so analysts can group "users with X stream"
    -- without re-joining to the per-stream sources.
    max(stream_type = 'holdings')                       AS has_holdings,
    max(stream_type = 'sdai')                           AS has_sdai,
    max(stream_type = 'gpay')                           AS has_gpay,
    uniqExact(stream_type)                              AS n_streams,
    -- Headline active-user flag — matches the Dune $6 / annual_rolling
    -- threshold used by fct_revenue_active_users_totals_weekly.
    sum(annual_rolling_fees) >= 6                       AS is_revenue_active
FROM {{ ref('int_revenue_fees_weekly_per_user') }}
WHERE user IS NOT NULL
  AND annual_rolling_fees > 0
GROUP BY week, user_pseudonym
