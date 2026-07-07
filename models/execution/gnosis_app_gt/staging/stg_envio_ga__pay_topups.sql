{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(card, funder)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'stretch', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'card_x_funder',
        'guard': 'STRETCH — full-scans envio_ga.transfer (108M, id-sorted so transfer_type does not prune). Reduced to the compact distinct (card=to, funder=from) set for PayTopUp/AutoTopup. Do NOT microbatch (each batch re-scans the whole table); built once per run.'
    }
) }}

-- STRETCH: one full scan of envio_ga.transfer, reduced to distinct (card, funder) for
-- app-initiated top-ups (PayTopUp / AutoTopup). funder (`from`) is the GA app account that
-- funded the card (`to`) through the app — a card -> GA-account link consumed by
-- int_execution_gnosis_app_gt_card_owner (gated there to registered GA accounts).
SELECT DISTINCT
    lower("to")   AS card,
    lower("from") AS funder
FROM {{ source('envio_ga', 'transfer') }}
WHERE _deleted = 0
  AND transfer_type IN ('PayTopUp', 'AutoTopup')
  AND "to"   != ''
  AND "from" != ''
