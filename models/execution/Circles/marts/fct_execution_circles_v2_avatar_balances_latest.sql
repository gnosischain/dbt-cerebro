{{
    config(
        materialized='table',
        tags=['production','execution','circles','v2','avatar','mart']
    )
}}

-- Latest per-(avatar, token) CRC balance snapshot with an is_wrapped flag
-- indicating whether the token_address is an ERC-20 wrapper. Materialised
-- daily from the per-day balance fact + wrapper registry; the api_ view
-- is a thin passthrough for dashboard consumption.

WITH latest AS (
    SELECT max(date) AS d
    FROM {{ ref('fct_execution_circles_v2_avatar_balances_daily') }}
    WHERE date < today()
),
unique_wrappers AS (
    SELECT DISTINCT lower(wrapper_address) AS wrapper_address
    FROM {{ ref('int_execution_circles_v2_wrappers') }}
)

SELECT
    b.avatar                              AS avatar,
    b.token_address                       AS token_address,
    w.wrapper_address IS NOT NULL         AS is_wrapped,
    b.balance                             AS balance,
    b.balance_demurraged                  AS balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_balances_daily') }} b
CROSS JOIN latest
LEFT JOIN unique_wrappers w
    ON w.wrapper_address = b.token_address
WHERE b.date = latest.d
