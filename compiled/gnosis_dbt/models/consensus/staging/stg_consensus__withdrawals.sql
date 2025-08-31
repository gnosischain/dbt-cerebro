

SELECT
    slot,
    block_number,
    block_hash,
    withdrawal_index,
    validator_index,
    address,
    amount,
    slot_timestamp
FROM 
    `consensus`.`withdrawals` FINAL