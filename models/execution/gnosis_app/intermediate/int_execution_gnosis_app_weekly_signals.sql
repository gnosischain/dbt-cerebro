{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','wau','weekly']
  )
}}

-- Composite weekly activity signal feeding the Gnosis App Weekly Active
-- Users metric. Mirrors the Dune circles-v2-kpis dashboard `weekly_active_avatars`
-- CTE: any address active in any of the three sources counts.
--
-- Three sources:
--   1. Circles v2 avatars (mints / trusts / human registration)
--      → int_execution_circles_v2_active_avatars_weekly
--   2. CoW PreSignature trades routed through Cometh bundlers that filled
--      → int_execution_gnosis_app_swaps (taker, was_filled = 1)
--   3. Gnosis App user activity (topup / marketplace_buy / token_offer_claim /
--      swap_signed / swap_filled — excluding `onboard`)
--      → int_execution_gnosis_app_user_activity_daily
--
-- Floor date is `var('gnosis_app_wau_floor_date')` (= Cometh v4 launch).
-- Earlier weeks are filtered out so the CoW signal doesn't see pre-launch
-- empty weeks at the start of the series.

{% set floor_date = var('gnosis_app_wau_floor_date') %}

WITH circles_signals AS (
    SELECT week, address
    FROM {{ ref('int_execution_circles_v2_active_avatars_weekly') }}
    WHERE week >= toDate('{{ floor_date }}')
),

cometh_swap_signals AS (
    -- Bucket by trade fill time (matches Dune: it groups on the Trade
    -- event timestamp, not the PreSignature timestamp).
    SELECT
        toStartOfWeek(first_fill_at, 1) AS week,
        taker                           AS address
    FROM {{ ref('int_execution_gnosis_app_swaps') }}
    WHERE was_filled = 1
      AND first_fill_at IS NOT NULL
      AND first_fill_at >= toDateTime('{{ floor_date }}')
      AND first_fill_at < today()
),

gpay_signals AS (
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
    SELECT week, address FROM circles_signals
    UNION ALL
    SELECT week, address FROM cometh_swap_signals
    UNION ALL
    SELECT week, address FROM gpay_signals
)
WHERE address != ''
  AND week < toStartOfWeek(today(), 1)
