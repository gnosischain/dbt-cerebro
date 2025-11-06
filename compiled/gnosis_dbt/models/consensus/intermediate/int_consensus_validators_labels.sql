

SELECT 
    validator_index
    ,pubkey
    ,withdrawal_credentials
FROM stg_consensus__validators
WHERE slot = (SELECT MAX(slot) FROM stg_consensus__validators)