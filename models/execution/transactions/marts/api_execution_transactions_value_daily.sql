{{ 
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:transactions_xdai_value', 'granularity:daily']
    )
}}

SELECT
    date
    ,transaction_type
    ,xdai_value 
    ,xdai_value_avg 
    ,xdai_value_median
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
ORDER BY date, transaction_type