

SELECT
    toDate(t.block_timestamp)                        AS date,
    coalesce(t.token_bought_symbol, wb.symbol)       AS token_bought_symbol,
    count(*)                                         AS n_topups,
    countDistinct(t.ga_user)                         AS n_ga_users,
    countDistinct(t.gp_wallet)                       AS n_gp_wallets,
    sum(t.amount_bought)                             AS volume_token_bought,
    sum(t.amount_usd)                                AS volume_usd
FROM `dbt`.`int_execution_gnosis_app_gpay_topups` t
LEFT JOIN `dbt`.`int_execution_circles_v2_wrapper_tokens` wb
    ON wb.wrapper_address = t.token_bought_address
GROUP BY date, token_bought_symbol
ORDER BY date, token_bought_symbol