

SELECT
    rank,
    token_address,
    symbol,
    token_class,
    address,
    label,
    label_sector,
    balance,
    balance_usd,
    pct_of_total,
    cumulative_pct,
    change_usd_7d,
    unwound_from,
    is_terminal_ubo
FROM `dbt`.`fct_execution_tokens_top_holders_latest`
ORDER BY token_address, rank