{{ 
    config(
            materialized='table'
        ) 
}}


WITH

gnosis_power_consumption AS (
    SELECT
        t1.date
        ,IF(t1.country='' OR t1.country='Unknown', 'Unknown', t2.name) AS country
        ,t1.power
        ,ROW_NUMBER() OVER (PARTITION BY t1.date ORDER BY t1.power DESC) AS rank
    FROM
        {{ ref('esg_country_power_consumption') }} t1
    LEFT JOIN
        {{ source('crawlers_data','country_codes') }} t2
        ON
        t2."alpha-2" = t1.country
)

SELECT
    date
    ,IF(rank>10, 'Other', country) AS country
    ,SUM(power) AS power
FROM
    gnosis_power_consumption
GROUP BY 
    1, 2