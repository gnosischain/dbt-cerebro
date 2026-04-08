{{
    config(
        materialized='table',
        tags=["production", "consensus", 'avalidators_status']
    )
}}

SELECT
    slot,
    validator_index,
    balance,
    status,
    lower(pubkey) AS pubkey,
    lower(withdrawal_credentials) AS withdrawal_credentials,
    effective_balance,
    slashed,
    activation_eligibility_epoch,
    activation_epoch,
    exit_epoch,
    withdrawable_epoch,
    slot_timestamp
FROM {{ ref('stg_consensus__validators_all') }}
WHERE slot = (SELECT MAX(slot) FROM {{ ref('stg_consensus__validators_all') }} )
ORDER BY validator_index
