

SELECT
    token_address,
    symbol,
    token_class,
    total_usd,
    pct_direct_terminal,
    pct_unwound_terminal,
    pct_unwound_other,
    pct_known_container,
    pct_unclassified,
    pct_unwound_total
FROM `dbt`.`fct_execution_tokens_ubo_coverage_latest`
ORDER BY total_usd DESC