{{
  config(
    materialized='table',
    tags=['dev','execution','tokens','top_holders']
  )
}}


WITH

prev_7d_date AS (
    SELECT addDays(max(date), -7) AS d
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date < today() AND balance > 0
),

prev_direct AS (
    SELECT
        r.token_address,
        r.address,
        coalesce(b.balance_usd, 0) AS balance_usd
    FROM {{ ref('fct_execution_tokens_top_holders_ranked') }} r
    LEFT JOIN (
        SELECT token_address, lower(address) AS address, balance_usd
        FROM {{ ref('int_execution_tokens_balances_daily') }}
        WHERE date = (SELECT d FROM prev_7d_date)
          AND balance > 0
    ) b ON b.token_address = r.token_address AND b.address = r.address
),

prev_ubo AS (
    SELECT
        r.token_address,
        r.address,
        coalesce(c.balance_usd, 0) AS balance_usd
    FROM {{ ref('fct_execution_tokens_top_holders_ranked') }} r
    LEFT JOIN (
        SELECT token_address, ubo_address AS address, balance_usd
        FROM {{ ref('fct_ubo_supply_claims_resolved_daily') }}
        WHERE date = (SELECT d FROM prev_7d_date)
          AND balance > 0
    ) c ON c.token_address = r.token_address AND c.address = r.address
),

prev_balances AS (
    SELECT
        token_address,
        address,
        sum(balance_usd) AS balance_usd_7d_ago
    FROM (
        SELECT token_address, address, balance_usd FROM prev_direct
        UNION ALL
        SELECT token_address, address, balance_usd FROM prev_ubo
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
    r.protocols,
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
