{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:withdrawal_credentials_frequency', 'granularity:daily']
    )
}}
SELECT 
    date
    ,bin_number_validators AS label
    ,withdrawal_credentials_freq_cnt AS value
FROM {{ ref('fct_consensus_withdrawal_credentials_freq_daily') }}
ORDER BY 1, 2