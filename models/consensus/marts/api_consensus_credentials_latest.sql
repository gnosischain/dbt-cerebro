{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:credentials', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_consensus_credentials_daily') }}) AS as_of_date
FROM (
SELECT 
    credentials_type
    ,cnt
FROM {{ ref('int_consensus_credentials_daily') }}
WHERE date = (SELECT MAX(date) FROM {{ ref('int_consensus_credentials_daily') }})
) AS sub
