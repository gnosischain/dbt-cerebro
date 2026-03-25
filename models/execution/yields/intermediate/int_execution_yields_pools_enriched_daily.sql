{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'daily']
    )
}}

{#-
  Central token-level balance enrichment for all LP pools.
  Joins raw balances with token metadata and prices (ASOF join for gap safety)
  to produce TVL components. Single source of truth for TVL across all
  downstream pool models.

  For Balancer V3 ERC4626 wrappers (waGno*), resolves to the underlying
  token for symbol and price lookups (wrapper ≈ 1:1 with underlying).
-#}

SELECT
    toDate(b.date) AS date,
    b.protocol AS protocol,
    concat('0x', replaceAll(lower(b.pool_address), '0x', '')) AS pool_address,
    replaceAll(lower(b.pool_address), '0x', '') AS pool_address_no0x,
    lower(b.token_address) AS token_address,
    tm.token AS token,
    b.reserve_amount AS token_amount,
    p.price_usd AS price_usd,
    b.reserve_amount * p.price_usd AS tvl_component_usd
FROM {{ ref('int_execution_pools_balances_daily') }} b
LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm
  ON wm.wrapper_address = lower(b.token_address)
LEFT JOIN {{ ref('stg_yields__tokens_meta') }} tm
  ON tm.token_address = coalesce(nullIf(wm.underlying_address, ''), lower(b.token_address))
 AND toDate(b.date) >= toDate(tm.date_start)
 AND (tm.date_end IS NULL OR toDate(b.date) < toDate(tm.date_end))
ASOF LEFT JOIN (
    SELECT * FROM {{ ref('stg_yields__token_prices_daily') }} ORDER BY token, date
) p
  ON p.token = tm.token
 AND toDate(b.date) >= p.date
WHERE b.date < today()
