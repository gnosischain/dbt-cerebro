

WITH

uni_swapr_lps AS (
    SELECT provider AS wallet_address
    FROM `dbt`.`int_execution_yields_user_lp_positions`
    WHERE protocol IN ('Uniswap V3', 'Swapr V3')
    GROUP BY wallet_address
    ORDER BY sum(fees_collected_usd) DESC
    LIMIT 25
),

both_lp_and_lending AS (
    SELECT wallet_address
    FROM `dbt`.`fct_execution_yields_user_lifetime_metrics`
    WHERE active_lp_positions > 0 AND active_lending_positions > 0
    ORDER BY (total_lp_fees_usd + total_lending_balance_usd) DESC
    LIMIT 25
),

top_lenders AS (
    SELECT wallet_address
    FROM `dbt`.`fct_execution_yields_user_lifetime_metrics`
    WHERE active_lending_positions > 0
    ORDER BY total_lending_balance_usd DESC
    LIMIT 25
),

combined AS (
    SELECT wallet_address, min(priority) AS priority
    FROM (
        SELECT wallet_address, 1 AS priority FROM uni_swapr_lps
        UNION ALL
        SELECT wallet_address, 2 AS priority FROM both_lp_and_lending
        UNION ALL
        SELECT wallet_address, 3 AS priority FROM top_lenders
    )
    GROUP BY wallet_address
)

SELECT wallet_address
FROM combined
ORDER BY priority
LIMIT 50