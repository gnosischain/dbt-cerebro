
-- A whitelisted token with real supply but no (or zero) USD price reads $0 on every
-- USD-valued surface — silently (coalesce(price, 0)). Catches new wrapper/vault
-- tokens before they render blank. Lesson: unpriced-wrapper-token (OC-sDAI: 265k
-- shares shown as $0). date = today()-2 tolerates the price feed's normal 1-day lag.
WITH latest_supply AS (
    SELECT symbol, token_class, argMax(supply, date) AS supply, max(date) AS supply_date
    FROM `dbt`.`int_execution_tokens_supply_holders_daily`
    WHERE date >= today() - 7
    GROUP BY symbol, token_class
    HAVING supply > 0
),
recent_prices AS (
    SELECT symbol, argMax(price, date) AS price
    FROM `dbt`.`int_execution_token_prices_daily`
    WHERE date >= today() - 2
    GROUP BY symbol
)
SELECT s.symbol, s.token_class, s.supply, s.supply_date, p.price
FROM latest_supply s
LEFT JOIN recent_prices p ON p.symbol = s.symbol
WHERE p.price IS NULL OR p.price = 0
SETTINGS join_use_nulls = 1