{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'avatars', 'api']
    )
}}

SELECT *
FROM {{ ref('fct_execution_circles_avatars_current') }}
