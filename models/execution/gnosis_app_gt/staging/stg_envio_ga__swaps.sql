{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- CoW order records (grain = order id). No fill/chain timestamp — do NOT build
-- a daily grain off this table (SWAP-D05). status is a REAL enum, never a
-- boolean. fee_amount is the CoW orderbook QUOTED fee (not settled revenue).
-- Amounts are native BE Int256 -> toFloat64 atoms (no reinterpret/reverse).
--
-- app_scope separates the app VERSIONS that share this Circles indexer:
-- app.gnosis.io (gnosis_app = the CURRENT Gnosis App), app.metri.xyz
-- (metri = the LEGACY Gnosis App), test appCodes, empty metadata (unknown), and
-- everything else (third-party CoW). gnosis_app + metri BOTH roll up to Gnosis
-- App (current + legacy): filter 'gnosis_app' for the current app only, or
-- include 'metri' for all-time Gnosis App.
SELECT
    id,
    order_uid,
    app_code,
    app_flow,
    multiIf(
        app_code = 'app.gnosis.io',                                    'gnosis_app',
        app_code = 'app.metri.xyz',                                    'metri',
        app_code IN ('CoW Swap Test', 'TestWallet') OR positionCaseInsensitive(app_code, 'test') > 0, 'test',
        app_code = '',                                                 'unknown',
        'third_party'
    )                                       AS app_scope,
    status,
    owner,
    receiver,
    sell_token,
    buy_token,
    sell_amount_atoms,
    buy_amount_atoms,
    fee_amount_atoms,
    transaction_hash
FROM (
    SELECT
        id,
        order_uid,
        JSONExtractString(app_data, 'appCode')  AS app_code,
        JSONExtractString(app_data, 'flow')     AS app_flow,
        status,
        lower(owner)                            AS owner,
        lower(receiver)                         AS receiver,
        lower(sell_token)                       AS sell_token,
        lower(buy_token)                        AS buy_token,
        toFloat64(sell_amount)                  AS sell_amount_atoms,
        toFloat64(buy_amount)                   AS buy_amount_atoms,
        toFloat64(fee_amount)                   AS fee_amount_atoms,
        transaction_hash
    FROM (
        {{ envio_latest(
            'envio_ga', 'swap',
            ['order_uid', 'app_data', 'status', 'owner', 'receiver', 'sell_token',
             'buy_token', 'sell_amount', 'buy_amount', 'fee_amount', 'transaction_hash']
        ) }}
    )
)
