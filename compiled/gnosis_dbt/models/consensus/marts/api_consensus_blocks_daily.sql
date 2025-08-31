


SELECT
    date
    ,label
    ,value
FROM (
    SELECT date, 'produced' AS label, blocks_produced AS value FROM `dbt`.`int_consensus_blocks_daily`
    UNION ALL 
    SELECT date, 'missed' AS label, blocks_missed AS value FROM `dbt`.`int_consensus_blocks_daily`
)
ORDER BY date, label