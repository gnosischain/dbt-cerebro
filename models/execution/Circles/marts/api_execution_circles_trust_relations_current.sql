{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'trusts', 'api']
    )
}}

SELECT *
FROM {{ ref('fct_execution_circles_trust_relations_current') }}
