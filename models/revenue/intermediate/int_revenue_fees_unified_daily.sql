{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross']
  )
}}

-- user is canonicalized through the June 2026 Safe migration so the same
-- person carries ONE address across all streams (gpay daily is already
-- canonical at the source; the remap is idempotent for it and covers the
-- holdings/sdai/gnosis_app streams here). CH LEFT JOIN fills '' on
-- misses, hence the empty-string guard.
WITH daily AS (
    SELECT 'holdings' AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_holdings_fees_daily') }}

    UNION ALL

    SELECT 'sdai'     AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_sdai_fees_daily') }}

    UNION ALL

    SELECT 'gpay'        AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_gpay_fees_daily') }}

    UNION ALL

    SELECT 'gnosis_app'  AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_gnosis_app_fees_daily') }}
)

SELECT
    d.stream_type AS stream_type,
    d.date        AS date,
    if(c.canonical_address != '', c.canonical_address, d.user) AS user,
    d.symbol      AS symbol,
    d.fees        AS fees
FROM daily d
LEFT JOIN {{ ref('int_execution_gpay_safe_canonical') }} c
    ON c.address = d.user
