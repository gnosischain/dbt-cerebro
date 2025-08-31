{{
    config(
        materialized='view',
        tags=["production", "consensus", "credentials"]
    )
}}

SELECT 
    credentials_type
    ,cnt
FROM {{ ref('int_consensus_credentials_daily') }}
WHERE date = (SELECT MAX(date) FROM {{ ref('int_consensus_credentials_daily') }})