{{ 
    config(
        materialized='view',
        tags=['production','execution','GBCDeposit']
    )
}}


SELECT 
    toStartOfDay(block_timestamp) AS date
    ,decoded_params['withdrawal_credentials'] AS withdrawal_credentials
    ,SUM(reinterpretAsUInt64(unhex(substring(decoded_params['amount'], 3)))) AS amount
FROM {{ ref('contracts_GBCDeposit_events') }}
WHERE
    event_name = 'DepositEvent'
    {{ apply_monthly_incremental_filter(source_field='block_timestamp',destination_field='date',add_and=true) }}
GROUP BY 1, 2
