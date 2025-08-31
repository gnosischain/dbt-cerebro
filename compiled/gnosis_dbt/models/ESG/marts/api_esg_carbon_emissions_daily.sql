


SELECT
    date,
    AVG(daily_co2_kg_mean) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_value,
    AVG(daily_co2_kg_lower_90) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_lower_90,
    AVG(daily_co2_kg_upper_90) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_upper_90,
    AVG(daily_co2_kg_lower_95) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_lower_95,
    AVG(daily_co2_kg_upper_95) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_upper_95
FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
WHERE toStartOfMonth(date) < toStartOfMonth(today())
ORDER BY date