{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(app_scope, sell_token, buy_token)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'mart']
) }}

-- FILLED swaps by app_scope x token pair. app_scope is a dimension (filter
-- 'gnosis_app' for the actual Gnosis App; 'metri' is a different app). Token
-- addresses only (swapper is an aggregate count). No USD (no price feed in
-- envio_ga) and no daily grain (no fill timestamp) — point-in-time.
SELECT
    app_scope,
    sell_token,
    buy_token,
    count()                     AS n_filled_swaps,
    uniqExact(owner)            AS n_swappers,
    sum(sell_amount_atoms)      AS sell_volume_atoms
FROM {{ ref('stg_envio_ga__swaps') }}
WHERE status = 'Filled'
GROUP BY app_scope, sell_token, buy_token
