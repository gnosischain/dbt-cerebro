

WITH

both_lp_and_lending AS (
    SELECT wallet_address
    FROM `dbt`.`fct_execution_yields_user_lifetime_metrics`
    WHERE active_lp_positions > 0 AND active_lending_positions > 0
    ORDER BY (total_lp_fees_usd + total_lending_balance_usd) DESC
    LIMIT 100
),

uni_swapr_lps AS (
    SELECT provider AS wallet_address
    FROM `dbt`.`int_execution_yields_user_lp_positions`
    WHERE protocol IN ('Uniswap V3', 'Swapr V3')
    GROUP BY wallet_address
    ORDER BY sum(fees_collected_usd) DESC
    LIMIT 100
),

top_lenders AS (
    SELECT wallet_address
    FROM `dbt`.`fct_execution_yields_user_lifetime_metrics`
    WHERE active_lending_positions > 0
    ORDER BY total_lending_balance_usd DESC
    LIMIT 100
),

combined AS (
    SELECT wallet_address FROM both_lp_and_lending
    UNION DISTINCT
    SELECT wallet_address FROM uni_swapr_lps
    UNION DISTINCT
    SELECT wallet_address FROM top_lenders
)

SELECT wallet_address
FROM combined
LIMIT 50