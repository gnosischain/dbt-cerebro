

SELECT
    slot,
    validator_index,
    balance,
    status,
    lower(pubkey) AS pubkey,
    lower(withdrawal_credentials) AS withdrawal_credentials,
    if(
        startsWith(lower(withdrawal_credentials), '0x01')
        OR startsWith(lower(withdrawal_credentials), '0x02'),
        concat('0x', substring(lower(withdrawal_credentials), 27, 40)),
        NULL
    ) AS withdrawal_address,
    effective_balance,
    slashed,
    activation_eligibility_epoch,
    activation_epoch,
    exit_epoch,
    withdrawable_epoch,
    slot_timestamp
FROM `dbt`.`stg_consensus__validators_all`
WHERE slot = (SELECT MAX(slot) FROM `dbt`.`stg_consensus__validators_all` )
ORDER BY validator_index