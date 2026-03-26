{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'groups', 'api']
    )
}}

SELECT *
FROM {{ ref('fct_execution_circles_groups_current') }}
