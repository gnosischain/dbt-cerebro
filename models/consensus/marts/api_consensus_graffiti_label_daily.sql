{{
    config(
        materialized='view',
        tags=["production", "consensus", "graffiti", 'tier1', 'api: graffiti_label_d']
    )
}}

SELECT
    date
    ,label
    ,SUM(cnt) AS value
FROM {{ ref('int_consensus_graffiti_daily') }}
GROUP BY date, label
ORDER BY date, label