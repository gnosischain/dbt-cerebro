{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross']
  )
}}

-- Per-user monthly revenue aggregation — collapses
-- int_revenue_fees_monthly_per_user (1 row per stream/symbol/user/month)
-- down to (month, user_pseudonym, month_fees_total, ...).
--
-- Privacy boundary: same as the weekly variant — raw `user` is hashed
-- to user_pseudonym and dropped. user_pseudonym is the canonical
-- cross-domain join key.
--
-- Used by the semantic layer for monthly cross-sector user-overlap
-- analysis. The $0.50 / month active-user threshold mirrors the
-- annual $6 from int_revenue_active_users_totals_weekly (month-scaled).

SELECT
    month,
    {{ pseudonymize_address('user') }}             AS user_pseudonym,
    round(sum(month_fees), 4)                      AS month_fees_total,
    max(stream_type = 'holdings')                  AS has_holdings,
    max(stream_type = 'sdai')                      AS has_sdai,
    max(stream_type = 'gpay')                      AS has_gpay,
    max(stream_type = 'gnosis_app')                AS has_gnosis_app,
    uniqExact(stream_type)                         AS n_streams,
    sum(month_fees) >= 0.5                         AS is_revenue_active
FROM {{ ref('int_revenue_fees_monthly_per_user') }}
WHERE user IS NOT NULL
  AND month_fees > 0
GROUP BY month, user_pseudonym
