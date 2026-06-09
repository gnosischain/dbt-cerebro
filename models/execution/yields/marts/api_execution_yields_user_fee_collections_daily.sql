{{
    config(
        materialized='view',
        tags=['production', 'execution', 'yields', 'api:yields_user_fee_collections', 'granularity:daily', 'tier1']
    )
}}

SELECT *
FROM {{ ref('fct_execution_yields_user_fee_collections_daily') }}
