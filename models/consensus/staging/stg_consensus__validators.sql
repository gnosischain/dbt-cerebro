
SELECT
    slot,
    validator_index,
    balance,
    status,
    pubkeyg,
    withdrawal_credentials,
    effective_balance,
    slashed,
    activation_eligibility_epoch,
    activation_epoch,
    exit_epoch,
    withdrawable_epoch,
    slot_timestamp
FROM 
    {{ source('consensus', 'validators') }} FINAL
