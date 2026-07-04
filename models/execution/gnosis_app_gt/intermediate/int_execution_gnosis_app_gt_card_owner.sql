{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(card, ga_account, source)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'card_x_ga_account_x_source',
        'guard': 'GP card (Safe) -> Gnosis App account link, union of three on-chain signals gated to the envio_ga registry (stg_envio_ga__users): Delay-module controller, cashback owner, app top-up funder. ga_account_pseudonym is the Mixpanel/user_pseudonym join key. Internal (raw addresses + pseudonym) — only aggregate downstream views are semantic-exposed.'
    }
) }}

-- Reusable card -> GA-account bridge. The funded/first_payment identity is the GP Safe owner
-- (relayer / exchange / GP-direct cardholder) that Mixpanel cannot see; this maps a card to the
-- human Gnosis App account(s) that actually control/fund it, so the conversion can reach a Mixpanel
-- user (and campaign). Three signals, all gated to a REGISTERED GA account:
--   delay_module  — Delay-module controller (int_execution_gnosis_app_gpay_wallets.first_ga_owner_address)
--   cashback      — cashback NFT owner linked to the card (stg_envio_ga__cashbacks)
--   topup_funder  — app top-up funder (stg_envio_ga__pay_topups, from -> card)
-- Measured ceiling: ~6.4% of recent funded cards link this way; cashback+top-ups add ~+108 beyond the
-- Delay path (the ~36k dangling cards fund from exchanges/bridges with no app identity in ANY source).

WITH reg AS (
    SELECT DISTINCT address AS ga_account FROM {{ ref('stg_envio_ga__users') }}
),

links AS (
    SELECT lower(pay_wallet) AS card, lower(first_ga_owner_address) AS ga_account, 'delay_module' AS source
    FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
    WHERE first_ga_owner_address IS NOT NULL AND first_ga_owner_address != ''

    UNION ALL

    SELECT DISTINCT lower(gnosis_pay_address) AS card, lower(owner) AS ga_account, 'cashback' AS source
    FROM {{ ref('stg_envio_ga__cashbacks') }}
    WHERE gnosis_pay_address != '' AND owner != ''
      AND lower(owner) IN (SELECT ga_account FROM reg)

    UNION ALL

    SELECT DISTINCT card, funder AS ga_account, 'topup_funder' AS source
    FROM {{ ref('stg_envio_ga__pay_topups') }}
    WHERE funder IN (SELECT ga_account FROM reg)
)

SELECT DISTINCT
    card,
    ga_account,
    source,
    {{ pseudonymize_address('ga_account') }} AS ga_account_pseudonym
FROM links
WHERE card != '' AND ga_account != ''
