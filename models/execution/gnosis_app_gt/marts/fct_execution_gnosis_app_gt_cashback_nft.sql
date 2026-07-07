{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(mint_date, status)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'mart']
) }}

-- Gnosis Pay cashback NFT-mint program — daily mint series x status (1=minted,
-- 0=reverted). A SEPARATE family from gpay_cashback_* (gCRC transfers) on a
-- disjoint time axis: NO USD/GNO value column, no shared axis, no pct_delta.
SELECT
    toDate(minted_at)   AS mint_date,
    status,
    count()             AS n_mints,
    uniqExact(owner)    AS n_owners
FROM {{ ref('stg_envio_ga__cashbacks') }}
GROUP BY mint_date, status
