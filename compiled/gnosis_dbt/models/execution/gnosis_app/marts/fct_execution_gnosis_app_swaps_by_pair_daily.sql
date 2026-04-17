

SELECT
    toDate(s.block_timestamp)                                                     AS date,
    concat(
        coalesce(s.token_sold_symbol, ws.symbol, '?'),
        ' → ',
        coalesce(s.token_bought_symbol, wb.symbol, '?')
    )                                                                             AS pair,
    coalesce(s.token_sold_symbol, ws.symbol)                                      AS token_sold_symbol,
    coalesce(s.token_bought_symbol, wb.symbol)                                    AS token_bought_symbol,
    count(*)                                                                      AS n_swaps,
    countIf(s.was_filled)                                                         AS n_swaps_filled,
    countDistinct(s.taker)                                                        AS n_swappers,
    sumIf(s.amount_usd, s.was_filled AND s.amount_usd IS NOT NULL)                AS volume_usd_filled
FROM `dbt`.`int_execution_gnosis_app_swaps` s
LEFT JOIN `dbt`.`int_execution_circles_v2_wrapper_tokens` ws
    ON ws.wrapper_address = s.token_sold_address
LEFT JOIN `dbt`.`int_execution_circles_v2_wrapper_tokens` wb
    ON wb.wrapper_address = s.token_bought_address
-- Pairs only make sense for filled orders (unfilled have no token addresses)
WHERE s.was_filled
GROUP BY date, pair, token_sold_symbol, token_bought_symbol
ORDER BY date, pair