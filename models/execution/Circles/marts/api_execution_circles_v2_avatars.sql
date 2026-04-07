{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatars', 'granularity:daily']
    )
}}

SELECT
    date,
    avatar_type,
    cnt,
    total
FROM {{ ref('fct_execution_circles_v2_avatars') }}
WHERE date < today()
ORDER BY date DESC, avatar_type
