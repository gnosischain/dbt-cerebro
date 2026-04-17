{{
    config(
        materialized='table',
        tags=['production','execution','circles','v2','avatar','mart']
    )
}}

-- One-row-per-avatar summary of a Circles v2 avatar's own personal CRC
-- token: total circulating supply, how much is wrapped as ERC-20, and
-- the wrapped share. Rolled up from fct_avatar_token_distribution so the
-- supply respects the same 0.001 CRC dust threshold used by the
-- distribution fact. The matching api_ is a thin passthrough.

SELECT
    avatar                                                                  AS avatar,
    sum(balance)                                                            AS supply,
    sumIf(balance, holder_category = 'Wrapped (ERC-20)')                    AS wrapped,
    sum(balance) - sumIf(balance, holder_category = 'Wrapped (ERC-20)')     AS unwrapped,
    round(sumIf(balance, holder_category = 'Wrapped (ERC-20)')
          / nullIf(sum(balance), 0) * 100, 1)                               AS wrapped_pct,
    sum(balance_demurraged)                                                 AS supply_demurraged,
    sumIf(balance_demurraged, holder_category = 'Wrapped (ERC-20)')         AS wrapped_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_token_distribution') }}
WHERE avatar IS NOT NULL
GROUP BY avatar
