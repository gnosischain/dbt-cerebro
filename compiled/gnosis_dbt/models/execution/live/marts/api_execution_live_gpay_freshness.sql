

-- Freshness for the GP live pulse: source lag (execution_live.logs) plus
-- the high-water mark of the cached GP activity table.

SELECT
    (SELECT max(block_timestamp) FROM `execution_live`.`logs`) AS newest_source_timestamp,
    (SELECT max(block_timestamp) FROM `dbt`.`int_live__gpay_activity_raw`) AS newest_gpay_timestamp,
    now()                                                                     AS server_now,
    dateDiff(
        'second',
        (SELECT max(block_timestamp) FROM `execution_live`.`logs`),
        now()
    )                                                                         AS source_lag_seconds,
    dateDiff(
        'second',
        (SELECT max(block_timestamp) FROM `dbt`.`int_live__gpay_activity_raw`),
        now()
    )                                                                         AS gpay_lag_seconds