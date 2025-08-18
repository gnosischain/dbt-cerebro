SELECT
    genesis_time_unix,
    seconds_per_slot,
    slots_per_epoch
FROM 
    `consensus`.`time_helpers` FINAL