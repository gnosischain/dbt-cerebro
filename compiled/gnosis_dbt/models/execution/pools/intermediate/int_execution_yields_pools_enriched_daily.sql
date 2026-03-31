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
FROM `dbt`.`int_execution_pools_balances_daily` b
LEFT JOIN `dbt`.`stg_pools__balancer_v3_token_map` wm
  ON wm.wrapper_address = lower(b.token_address)
LEFT JOIN `dbt`.`stg_yields__tokens_meta` tm
  ON tm.token_address = coalesce(nullIf(wm.underlying_address, ''), lower(b.token_address))
 AND toDate(b.date) >= toDate(tm.date_start)
ASOF LEFT JOIN (
    SELECT * FROM `dbt`.`stg_yields__token_prices_daily` ORDER BY token, date
) p
  ON p.token = tm.token
 AND toDate(b.date) >= p.date
WHERE b.date < today()