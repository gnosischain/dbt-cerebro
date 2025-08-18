SELECT
   slot,
    deposit_index,
    pubkey,
    withdrawal_credentials,
    amount,
    signature,
    proof,
    slot_timestamp
FROM 
    `consensus`.`deposits` FINAL