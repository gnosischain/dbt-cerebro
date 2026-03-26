{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'tokens', 'api']
    )
}}

SELECT *
FROM {{ ref('fct_execution_circles_tokens_current') }}
