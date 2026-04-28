

SELECT 'holdings' AS stream_type, date, user, symbol, fees
FROM `dbt`.`int_revenue_holdings_fees_daily`

UNION ALL

SELECT 'sdai'     AS stream_type, date, user, symbol, fees
FROM `dbt`.`int_revenue_sdai_fees_daily`

UNION ALL

SELECT 'gpay'     AS stream_type, date, user, symbol, fees
FROM `dbt`.`int_revenue_gpay_fees_daily`