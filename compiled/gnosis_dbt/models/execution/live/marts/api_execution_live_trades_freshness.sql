

SELECT
    max(block_timestamp)                                    AS newest_block_timestamp,
    now()                                                   AS server_now,
    dateDiff('second', max(block_timestamp), now())         AS lag_seconds
FROM `execution_live`.`logs`