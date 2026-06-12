{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:graffities_labels', 'granularity:in_ranges']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_consensus_graffiti_daily') }}) AS as_of_date
FROM (
SELECT
    label
    ,graffiti
    ,value
FROM {{ ref('fct_consensus_graffiti_cloud') }}
ORDER BY label DESC, value DESC
) AS sub
