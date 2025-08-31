

SELECT
    slot,
    payload,
    deposits_count,
    withdrawals_count,
    consolidations_count,
    slot_timestamp
FROM 
    `consensus`.`execution_requests` FINAL