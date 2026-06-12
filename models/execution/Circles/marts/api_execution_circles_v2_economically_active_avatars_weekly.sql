{{
    config(
        materialized='view',
        tags=['production','execution','circles_v2','tier1',
              'api:circles_v2_economically_active_avatars', 'granularity:weekly']
    )
}}

SELECT
    week,
    earning_kind,
    avatars,
    avatars_in_app_tx
FROM {{ ref('fct_execution_circles_v2_economically_active_avatars_weekly') }}
ORDER BY week DESC, earning_kind
