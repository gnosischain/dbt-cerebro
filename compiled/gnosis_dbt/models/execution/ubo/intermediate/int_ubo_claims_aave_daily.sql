

-- Per-protocol UBO supply claims for Aave V3 + SparkLend.
--
-- This is the standardized "who can withdraw what" projection of
-- int_execution_lending_aave_user_balances_daily — same per-user balances,
-- reshaped into the protocol-agnostic supply-claim schema that
-- fct_ubo_supply_claims_daily unions across protocols.
--
-- Phase 2 protocols (Balancer, Curve, …) will each have their own sibling
-- int_ubo_claims_<protocol>_daily model contributing rows in the same shape.




WITH

mapping AS (
    SELECT
        protocol,
        lower(reserve_address)      AS reserve_address,
        lower(supply_token_address) AS container_address,
        reserve_symbol              AS symbol,
        token_class
    FROM `dbt`.`lending_market_mapping`
)

SELECT
    u.date                                          AS date,
    u.protocol                                      AS protocol,
    m.container_address                             AS container_address,
    lower(coalesce(tw.address, u.reserve_address))  AS token_address,
    m.symbol                                        AS symbol,
    m.token_class                                   AS token_class,
    lower(u.user_address)                           AS ubo_address,
    toInt256(u.balance_raw)                         AS balance_raw,
    u.balance                                       AS balance,
    u.balance_usd                                   AS balance_usd
FROM `dbt`.`int_execution_lending_aave_user_balances_daily` u
INNER JOIN mapping m
    ON  m.protocol        = u.protocol
    AND m.reserve_address = lower(u.reserve_address)
LEFT JOIN `dbt`.`tokens_whitelist` tw
    ON  lower(tw.symbol)  = lower(m.symbol)
    AND u.date            >= tw.date_start
    AND (tw.date_end IS NULL OR u.date < tw.date_end)
WHERE u.date < today()
  AND u.balance > 0
  
    
  

  