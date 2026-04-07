{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_balances_latest', 'granularity:snapshot']
    )
}}

WITH latest AS (
    SELECT max(date) AS d
    FROM {{ ref('int_execution_circles_v2_balances_daily') }}
    WHERE date < today()
),
-- Wrapper addresses are 1:1 with token contracts on-chain, but the
-- upstream ERC20WrapperDeployed event has been emitted twice for some
-- wrappers (same wrapper_address, same avatar, same circles_type).
-- DISTINCT collapses those duplicates so the LEFT JOIN below is
-- guaranteed 1:0-or-1 and balance rows do not multiply.
unique_wrappers AS (
    SELECT DISTINCT lower(wrapper_address) AS wrapper_address
    FROM {{ ref('int_execution_circles_v2_wrappers') }}
)

SELECT
    b.account                                          AS avatar,
    b.token_address                                    AS token_address,
    w.wrapper_address IS NOT NULL                      AS is_wrapped,
    toFloat64(b.balance_raw)            / pow(10, 18)  AS balance,
    toFloat64(b.demurraged_balance_raw) / pow(10, 18)  AS balance_demurraged
FROM {{ ref('int_execution_circles_v2_balances_daily') }} b
CROSS JOIN latest
LEFT JOIN unique_wrappers w
    ON w.wrapper_address = b.token_address
WHERE b.date = latest.d
  AND b.balance_raw > POW(10, 15)
