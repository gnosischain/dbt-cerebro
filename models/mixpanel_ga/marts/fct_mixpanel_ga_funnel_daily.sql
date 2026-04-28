{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    tags=['production', 'mixpanel_ga']
  )
}}

-- Day-scoped funnel conversion metrics for Gnosis App (app.gnosis.io).
-- Three funnels are computed per day using uniqExact(user_id_hash) as the
-- unit of measurement, giving DAU-based conversion rates.
--
-- Onboarding funnel:
--   welcome_visitors      users on /welcome (entry point for new users)
--   join_modal_opens      users who opened ModalJoin (next step)
--   passkey_logins        users who completed Login with Passkey
--   identified_users      users with a wallet-identified session (is_identified)
--
-- Swap funnel:
--   swap_page_visitors    users on any /swap/* page
--   swap_asset_selectors  users who opened SelectSwapAsset modal
--   swap_completions      users who clicked the Swap button (ariaLabel = 'Swap')
--
-- Circles funnel:
--   circles_visitors      users on /circles
--   circles_mint_visitors users on /circles/mint
--   circles_mints         users who triggered 'Success - Circles mint'

WITH daily AS (
    SELECT
        event_date                                              AS date,
        event_name,
        event_category,
        page_path,
        bottom_sheet,
        is_identified,
        is_autocapture,
        user_id_hash
    FROM {{ ref('stg_mixpanel_ga__events') }}
    WHERE is_production = 1
      AND event_date < today()
)

SELECT
    date,

    -- ── Onboarding funnel ────────────────────────────────────────────────
    uniqExactIf(user_id_hash, page_path = '/welcome')                   AS welcome_visitors,
    uniqExactIf(user_id_hash, bottom_sheet = 'ModalJoin')               AS join_modal_opens,
    uniqExactIf(user_id_hash, event_name = 'Login with Passkey')        AS passkey_logins,
    uniqExactIf(user_id_hash, is_identified = 1)                        AS identified_users,

    round(
        uniqExactIf(user_id_hash, event_name = 'Login with Passkey')
        / greatest(uniqExactIf(user_id_hash, page_path = '/welcome'), 1),
        4
    )                                                                   AS onboarding_conversion_rate,

    -- ── Swap funnel ──────────────────────────────────────────────────────
    uniqExactIf(user_id_hash, startsWith(page_path, '/swap'))           AS swap_page_visitors,
    uniqExactIf(user_id_hash, bottom_sheet = 'SelectSwapAsset')         AS swap_asset_selectors,
    uniqExactIf(
        user_id_hash,
        event_name = 'Swap' AND is_autocapture = 1
    )                                                                   AS swap_completions,

    round(
        uniqExactIf(user_id_hash, event_name = 'Swap' AND is_autocapture = 1)
        / greatest(uniqExactIf(user_id_hash, startsWith(page_path, '/swap')), 1),
        4
    )                                                                   AS swap_conversion_rate,

    -- ── Circles funnel ───────────────────────────────────────────────────
    uniqExactIf(user_id_hash, page_path = '/circles')                   AS circles_visitors,
    uniqExactIf(user_id_hash, page_path = '/circles/mint')              AS circles_mint_visitors,
    uniqExactIf(user_id_hash, event_name = 'Success - Circles mint')    AS circles_mints,

    round(
        uniqExactIf(user_id_hash, event_name = 'Success - Circles mint')
        / greatest(uniqExactIf(user_id_hash, page_path = '/circles'), 1),
        4
    )                                                                   AS circles_conversion_rate

FROM daily
GROUP BY date
ORDER BY date
