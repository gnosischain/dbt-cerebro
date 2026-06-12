

SELECT
    toDate(block_timestamp)             AS date,
    crc20_token,
    backing_token,
    backing_token_symbol,
    pool_address,
    protocol,
    avatar,
    circles_type,
    crc20_symbol,
    avg(price_in_backing)               AS price_avg_in_backing,
    median(price_in_backing)            AS price_median_in_backing,
    quantile(0.1)(price_in_backing)     AS price_p10_in_backing,
    quantile(0.9)(price_in_backing)     AS price_p90_in_backing,
    sum(crc_amount)                     AS crc_volume,
    sum(crc_bought_amount)              AS crc_bought_volume,
    sum(crc_sold_amount)                AS crc_sold_volume,
    count()                             AS trade_count,
    avg(amount_usd / NULLIF(crc_amount, 0))    AS price_avg_usd,
    median(amount_usd / NULLIF(crc_amount, 0)) AS price_median_usd
FROM `dbt`.`int_execution_circles_v2_crc20_prices_raw`
GROUP BY
    date, crc20_token, backing_token, backing_token_symbol,
    pool_address, protocol, avatar, circles_type, crc20_symbol