{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_balances_latest', 'granularity:snapshot']
    )
}}

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
    b.avatar,
    b.token_address,
    w.wrapper_address IS NOT NULL AS is_wrapped,
    b.balance,
    b.balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_balances_daily') }} b
CROSS JOIN latest
LEFT JOIN unique_wrappers w
    ON w.wrapper_address = b.token_address
WHERE b.date = latest.d
