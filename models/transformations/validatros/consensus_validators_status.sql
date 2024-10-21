{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    ) 
}}

WITH

validators AS (
    SELECT
        f_index
        ,f_withdrawal_credentials
        ,{{ compute_timestamp_at_epoch('f_activation_eligibility_epoch') }} AS activation_eligibility_time
        ,{{ compute_timestamp_at_epoch('f_activation_epoch') }} AS activation_time
        ,{{ compute_timestamp_at_epoch('f_exit_epoch') }} AS exit_time
        ,{{ compute_timestamp_at_epoch('f_withdrawable_epoch') }} AS withdrawable_time
    FROM
        {{ get_postgres('gnosis_chaind', 't_validators') }}
    {% if is_incremental() %}
    WHERE f_inclusion_slot > (SELECT max(f_inclusion_slot) FROM {{ this }})
    {% endif %}
)

SELECT * FROM validators