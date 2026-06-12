{{
  config(
    materialized='table',
    tags=['production','execution','tokens','ubo','coverage']
  )
}}

-- Per-token diagnostic: how much of each token's supply is resolved to a
-- real end-holder (UBO) vs. still sitting in an undecomposed container vs.
-- unclassified.
--
--   pct_direct_terminal   — direct holders that are label-confirmed terminals
--                           (EOAs, Safes/AA, bridges, gpay)
--   pct_unwound_terminal  — addresses unwound out of a container that are
--                           themselves terminals
--   pct_unwound_other     — unwound addresses that are NOT label-confirmed
--                           terminals
--   pct_known_container   — supply still held by labeled containers we
--                           have NOT yet decomposed (Phase 2+ targets)
--   pct_unclassified      — supply held by addresses with no label
--   pct_unwound_total     — total supply that flowed through any unwind path

WITH

direct_rows AS (
    SELECT
        b.token_address                      AS token_address,
        b.symbol                             AS symbol,
        b.token_class                        AS token_class,
        lower(b.address)                     AS address,
        b.balance_usd                        AS balance_usd,
        CAST([] AS Array(String))            AS unwound_from
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    LEFT ANTI JOIN {{ ref('fct_ubo_known_containers_daily') }} k
        ON  k.date                     = b.date
        AND lower(k.token_address)     = lower(b.token_address)
        AND lower(k.container_address) = lower(b.address)
    WHERE b.date = (
            SELECT max(date)
            FROM {{ ref('int_execution_tokens_balances_daily') }}
            WHERE date < today() AND balance > 0
          )
      AND b.balance > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
),

unwound_rows AS (
    SELECT
        c.token_address                      AS token_address,
        c.symbol                             AS symbol,
        c.token_class                        AS token_class,
        c.ubo_address                        AS address,
        c.balance_usd                        AS balance_usd,
        [c.container_address]                AS unwound_from
    FROM {{ ref('fct_ubo_supply_claims_resolved_daily') }} c
    WHERE c.date = (
            SELECT max(date)
            FROM {{ ref('int_execution_tokens_balances_daily') }}
            WHERE date < today() AND balance > 0
          )
      AND c.balance > 0
),

combined AS (
    SELECT token_address, symbol, token_class, address, balance_usd, unwound_from
    FROM direct_rows
    UNION ALL
    SELECT token_address, symbol, token_class, address, balance_usd, unwound_from
    FROM unwound_rows
),

per_holder AS (
    SELECT
        token_address,
        any(symbol)                                  AS symbol,
        any(token_class)                             AS token_class,
        address,
        sum(balance_usd)                             AS balance_usd,
        arrayDistinct(groupArrayArray(unwound_from)) AS unwound_from
    FROM combined
    GROUP BY token_address, address
),

classified AS (
    SELECT
        h.token_address,
        h.symbol,
        h.token_class,
        h.balance_usd,
        h.unwound_from,
        l.is_terminal_ubo
    FROM per_holder h
    LEFT JOIN {{ ref('fct_ubo_address_classification') }} l
        ON l.address = h.address
),

per_token AS (
    SELECT
        token_address,
        symbol,
        token_class,
        sumIf(balance_usd, length(unwound_from) = 0 AND is_terminal_ubo = 1) AS direct_terminal_usd,
        sumIf(balance_usd, length(unwound_from) > 0 AND is_terminal_ubo = 1) AS unwound_terminal_usd,
        sumIf(balance_usd, length(unwound_from) > 0 AND (is_terminal_ubo IS NULL OR is_terminal_ubo = 0)) AS unwound_other_usd,
        sumIf(balance_usd, length(unwound_from) = 0 AND is_terminal_ubo = 0) AS known_container_usd,
        sumIf(balance_usd, length(unwound_from) = 0 AND is_terminal_ubo IS NULL) AS unclassified_usd,
        sumIf(balance_usd, length(unwound_from) > 0)                            AS total_unwound_usd,
        sum(balance_usd)                                                          AS total_usd
    FROM classified
    GROUP BY token_address, symbol, token_class
)

SELECT
    token_address,
    symbol,
    token_class,
    total_usd,
    round(direct_terminal_usd  / nullIf(total_usd, 0) * 100, 2) AS pct_direct_terminal,
    round(unwound_terminal_usd / nullIf(total_usd, 0) * 100, 2) AS pct_unwound_terminal,
    round(unwound_other_usd    / nullIf(total_usd, 0) * 100, 2) AS pct_unwound_other,
    round(known_container_usd  / nullIf(total_usd, 0) * 100, 2) AS pct_known_container,
    round(unclassified_usd     / nullIf(total_usd, 0) * 100, 2) AS pct_unclassified,
    round(total_unwound_usd    / nullIf(total_usd, 0) * 100, 2) AS pct_unwound_total
FROM per_token
ORDER BY total_usd DESC
