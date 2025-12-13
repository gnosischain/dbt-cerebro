{{ 
    config(
        materialized='view',
        tags=['production','execution','circles','avatars', 'tier0', 'api: avatars']
    )
}}

SELECT
    date
    ,avatar_type
    ,cnt
    ,total
FROM {{ ref('fct_execution_circles_avatars') }}
ORDER BY date, avatar_type
