{{ 
    config(
            materialized='table'
        ) 
}}


WITH

gnosis_power_consumption AS (
    SELECT
        hour
        ,country_code
        ,power
    FROM
        {{ ref('gnosis_power_consumption') }}
),

ember_data AS (
    SELECT
        "Date" AS month_date
        ,"Value" AS value
        ,LAG("Value") OVER (PARTITION BY "Country_code" ORDER BY "Date")  AS lag_value
        ,"Country_code" AS country_code
    FROM
        {{ ref('ember_electricity_data') }}
    WHERE   
        "Unit" = 'gCO2/kWh'

)

SELECT
    t1.hour
    ,SUM(t1.power * 1) AS energy
    ,SUM(t1.power * 1 * COALESCE(t3.value,t3.lag_value)) AS co2_emissions
    ,AVG(COALESCE(t3.value,t3.lag_value)) AS mean_cif
FROM
    gnosis_power_consumption t1
LEFT JOIN
    {{ ref('country_codes') }} t2
    ON
    t2."alpha-2" = t1.country_code
LEFT JOIN
    ember_data t3
    ON
    t3.country_code = t2."alpha-3"
    AND
    t3.month_date = DATE_TRUNC('month', t1.hour)
GROUP BY 
    t1.hour

