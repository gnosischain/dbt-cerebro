{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:graffities_labels', 'granularity:daily']
    )
}}

SELECT
    date
    ,label
    ,SUM(cnt) AS value
FROM {{ ref('int_consensus_graffiti_daily') }}
GROUP BY date, label
ORDER BY date, label