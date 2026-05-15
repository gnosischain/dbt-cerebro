{{
  config(
    materialized='table',
    tags=['dev','execution','tokens','top_holders','ranked']
  )
}}

-- Latest-day token holder ranking with UBO unwinding. Container-contract
-- holders (Aave aTokens for Phase 1; Balancer/Curve for Phase 2) are
-- replaced with the individual end-holders inside them via
-- fct_ubo_supply_claims_daily + fct_ubo_known_containers_daily.

{% set max_rank = 500 %}

WITH

latest_date AS (
    SELECT max(date) AS d
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date < today() AND balance > 0
),

direct_rows AS (
    SELECT
        b.token_address                      AS token_address,
        b.symbol                             AS symbol,
        b.token_class                        AS token_class,
        lower(b.address)                     AS address,
        b.balance                            AS balance,
        b.balance_usd                        AS balance_usd,
        CAST([] AS Array(String))            AS unwound_from
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    CROSS JOIN latest_date ld
    LEFT ANTI JOIN {{ ref('fct_ubo_known_containers_daily') }} k
        ON  k.date                     = b.date
        AND lower(k.token_address)     = lower(b.token_address)
        AND lower(k.container_address) = lower(b.address)
    WHERE b.date = ld.d
      AND b.balance > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
),

unwound_rows AS (
    SELECT
        c.token_address                      AS token_address,
        c.symbol                             AS symbol,
        c.token_class                        AS token_class,
        c.ubo_address                        AS address,
        c.balance                            AS balance,
        c.balance_usd                        AS balance_usd,
        [c.container_address]                AS unwound_from
    FROM {{ ref('fct_ubo_supply_claims_daily') }} c
    CROSS JOIN latest_date ld
    WHERE c.date = ld.d
      AND c.balance > 0
),

combined AS (
    SELECT
        token_address, symbol, token_class, address,
        balance, balance_usd, unwound_from
    FROM direct_rows

    UNION ALL

    SELECT
        token_address, symbol, token_class, address,
        balance, balance_usd, unwound_from
    FROM unwound_rows
),

per_holder AS (
    SELECT
        token_address,
        any(symbol)                                  AS symbol,
        any(token_class)                             AS token_class,
        address,
        sum(balance)                                 AS balance,
        sum(balance_usd)                             AS balance_usd,
        arrayDistinct(groupArrayArray(unwound_from)) AS unwound_from
    FROM combined
    GROUP BY token_address, address
),

ranked AS (
    SELECT
        token_address,
        symbol,
        token_class,
        address,
        balance,
        balance_usd,
        unwound_from,
        balance_usd / nullIf(sum(balance_usd) OVER (PARTITION BY token_address), 0) * 100
            AS pct_of_total,
        row_number() OVER (PARTITION BY token_address ORDER BY balance_usd DESC) AS rank
    FROM per_holder
)

SELECT *
FROM ranked
WHERE rank <= {{ max_rank }}
