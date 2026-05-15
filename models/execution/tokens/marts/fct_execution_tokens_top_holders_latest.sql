{{
  config(
    materialized='table',
    tags=['dev','execution','tokens','top_holders']
  )
}}

-- Latest-day top holders with labels, cumulative %, and 7-day USD delta.
-- Both today and (today - 7) are UBO-aware: the prev_balances CTE replays
-- the same unwind as fct_execution_tokens_top_holders_ranked but for the
-- 7-day-ago snapshot, so deltas track the UBO's own historical position
-- rather than the container contract's.

WITH

prev_date AS (
    SELECT max(date) - 7 AS d
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date < today() AND balance > 0
),

prev_direct_rows AS (
    SELECT
        b.token_address                      AS token_address,
        lower(b.address)                     AS address,
        b.balance_usd                        AS balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    CROSS JOIN prev_date pd
    LEFT ANTI JOIN {{ ref('fct_ubo_known_containers_daily') }} k
        ON  k.date                     = b.date
        AND lower(k.token_address)     = lower(b.token_address)
        AND lower(k.container_address) = lower(b.address)
    WHERE b.date = pd.d
      AND b.balance > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
),

prev_unwound_rows AS (
    SELECT
        c.token_address                      AS token_address,
        c.ubo_address                        AS address,
        c.balance_usd                        AS balance_usd
    FROM {{ ref('fct_ubo_supply_claims_daily') }} c
    CROSS JOIN prev_date pd
    WHERE c.date = pd.d
      AND c.balance > 0
),

prev_combined AS (
    SELECT token_address, address, balance_usd FROM prev_direct_rows
    UNION ALL
    SELECT token_address, address, balance_usd FROM prev_unwound_rows
),

prev_balances AS (
    SELECT
        token_address,
        address,
        sum(balance_usd) AS balance_usd_7d_ago
    FROM prev_combined
    WHERE (token_address, address) IN (
        SELECT token_address, address
        FROM {{ ref('fct_execution_tokens_top_holders_ranked') }}
    )
    GROUP BY token_address, address
)

SELECT
    r.rank,
    r.token_address AS token_address,
    r.symbol,
    r.token_class,
    r.address AS address,
    l.project AS label,
    l.sector AS label_sector,
    r.balance,
    r.balance_usd,
    round(r.pct_of_total, 4) AS pct_of_total,
    round(sum(r.pct_of_total) OVER (
        PARTITION BY r.token_address ORDER BY r.rank
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 4) AS cumulative_pct,
    r.balance_usd - coalesce(p.balance_usd_7d_ago, 0) AS change_usd_7d,
    r.unwound_from,
    CASE
        WHEN l.sector IN ('EOAs', 'Wallets & AA', 'Bridges', 'Payments') THEN toNullable(toUInt8(1))
        WHEN l.sector IN ('Lending & Yield', 'DEX')                      THEN toNullable(toUInt8(0))
        ELSE CAST(NULL AS Nullable(UInt8))
    END AS is_terminal_ubo
FROM {{ ref('fct_execution_tokens_top_holders_ranked') }} r
LEFT JOIN prev_balances p
    ON r.address = p.address
   AND r.token_address = p.token_address
LEFT JOIN {{ ref('int_crawlers_data_labels') }} l
    ON lower(l.address) = lower(r.address)
ORDER BY r.token_address, r.rank
