WITH

gnosis_power_consumption AS (
    SELECT
        date
        ,country
        ,power
    FROM
        `dbt`.`int_esg_country_power_consumption`
),

ember_data AS (
    SELECT
        "Date" AS month_date
        ,"Value" AS value
        ,lagInFrame("Value") OVER (PARTITION BY "ISO 3 code" ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS lag_value
        ,"ISO 3 code" AS country
    FROM
        `crawlers_data`.`ember_electricity_data`
    WHERE   
        "Unit" = 'gCO2/kWh'

)

SELECT
    t1.date
    ,SUM(t1.power * 24)/POWER(10,6) AS energy -- MWh
    ,SUM(
        t1.power/POWER(10,3)  -- power in kW
        * 24 -- hours in day
        * COALESCE(t3.value,t3.lag_value) -- CIF in gCO2/kWh
        )/POWER(10,6) AS co2_emissions -- in tCO2e
    ,AVG(COALESCE(t3.value,t3.lag_value)) AS mean_cif
FROM
    gnosis_power_consumption t1
LEFT JOIN
    `crawlers_data`.`country_codes` t2
    ON
    t2."alpha-2" = t1.country
INNER JOIN
    ember_data t3
    ON
    t3.country = t2."alpha-3"
    AND
    t3.month_date = toStartOfMonth(t1.date)
GROUP BY 
    1