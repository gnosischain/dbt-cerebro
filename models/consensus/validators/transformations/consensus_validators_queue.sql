{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(f_eth1_block_timestamp, f_validator_pubkey)',
        primary_key='(f_eth1_block_timestamp, f_validator_pubkey)',
        partition_by='partition_month'
    ) 
}}

SELECT 
    toStartOfMonth(el.f_eth1_block_timestamp) AS partition_month
    ,v.f_index
    ,el.f_validator_pubkey AS f_validator_pubkey
    ,el.f_withdrawal_credentials
    ,el.f_signature
    ,el.f_eth1_block_timestamp AS f_eth1_block_timestamp
    ,el.f_eth1_gas_used
    ,el.f_eth1_gas_price
    ,el.f_amount
    ,{{ compute_timestamp_at_slot('cl.f_inclusion_slot') }} AS inclusion_time
    ,{{ compute_timestamp_at_epoch('v.f_activation_eligibility_epoch') }} AS activation_eligibility_time
    ,{{ compute_timestamp_at_epoch('v.f_activation_epoch') }} AS activation_time
    ,{{ compute_timestamp_at_slot('ve.f_inclusion_slot') }} AS exit_request_time
    ,{{ compute_timestamp_at_epoch('ve.f_epoch') }} AS exit_voluntary_time
    ,{{ compute_timestamp_at_epoch('v.f_exit_epoch') }} AS exit_time
    ,{{ compute_timestamp_at_epoch('v.f_withdrawable_epoch') }} AS withdrawable_time
FROM 
    {{ get_postgres('chaind', 't_eth1_deposits') }} el
LEFT JOIN
    {{ get_postgres('chaind', 't_deposits') }} cl
    ON cl.f_validator_pubkey = el.f_validator_pubkey
INNER JOIN
    {{ get_postgres('chaind', 't_validators') }} v
    ON v.f_public_key = el.f_validator_pubkey
LEFT JOIN
    {{ get_postgres('chaind', 't_voluntary_exits') }} ve
    ON ve.f_validator_index = v.f_index
{% if is_incremental() %}
WHERE 
    toStartOfWeek(el.f_eth1_block_timestamp) >= (SELECT max(partition_week) FROM {{ this }})
{% endif %}
SETTINGS 
    join_use_nulls = 1