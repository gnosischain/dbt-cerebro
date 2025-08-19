SELECT
    date
    ,label
    ,SUM(cnt) AS value
FROM {{ ref('int_consensus_graffiti_daily') }}
GROUP BY date, label
ORDER BY date, label