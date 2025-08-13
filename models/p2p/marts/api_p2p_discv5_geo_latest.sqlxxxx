
SELECT
    date
    ,lat
    ,long
    ,country
    ,cnt
FROM {{ ref('int_p2p_discv5_geo_daily') }}
WHERE
    date = least((SELECT MAX(toStartOfDay(date)) FROM {{ ref('int_p2p_discv5_geo_daily') }}),today())