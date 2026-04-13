{{
    config(
        materialized='view',
        tags=['production','execution','yields','api:yields_user_fee_collections_daily','granularity:daily']
    )
}}

SELECT *
FROM {{ ref('fct_execution_yields_user_fee_collections_daily') }}
