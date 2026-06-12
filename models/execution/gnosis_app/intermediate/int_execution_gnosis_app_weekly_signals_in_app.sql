{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','wau','weekly']
  )
}}

-- IN-APP-ONLY weekly activity signal: addresses active via actions taken in
-- the Gnosis App. Companion to int_execution_gnosis_app_weekly_signals (the
-- ecosystem-wide WAU feed) — same shape, minus the global Circles avatar leg.
--
-- Why dropping that leg is sufficient: in-app Circles actions (register,
-- trust, personal mint, invite, fee, profile update) are already captured by
-- the Cometh-relayer heuristics inside
-- int_execution_gnosis_app_user_activity_daily, which this model keeps. The
-- global avatar feed only ADDS out-of-app Circles activity, which is exactly
-- what this variant excludes.
--
-- Two sources:
--   1. CoW PreSignature trades routed through Cometh bundlers that filled
--      → int_execution_gnosis_app_swaps (taker, was_filled = 1)
--   2. Gnosis App user activity (circles heuristics / topup / marketplace_buy /
--      token_offer_claim / swap_signed / swap_filled — excluding `onboard`)
--      → int_execution_gnosis_app_user_activity_daily

{% set floor_date = var('gnosis_app_wau_floor_date') %}

WITH cometh_swap_signals AS (
    SELECT
        toStartOfWeek(first_fill_at, 1) AS week,
        taker                           AS address
    FROM {{ ref('int_execution_gnosis_app_swaps') }}
    WHERE was_filled = 1
      AND first_fill_at IS NOT NULL
      AND first_fill_at >= toDateTime('{{ floor_date }}')
      AND first_fill_at < today()
),

app_activity_signals AS (
    SELECT
        toStartOfWeek(date, 1) AS week,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind != 'onboard'
      AND date >= toDate('{{ floor_date }}')
      AND date < today()
)

SELECT DISTINCT week, address
FROM (
    SELECT week, address FROM cometh_swap_signals
    UNION ALL
    SELECT week, address FROM app_activity_signals
)
WHERE address != ''
  AND week < toStartOfWeek(today(), 1)
