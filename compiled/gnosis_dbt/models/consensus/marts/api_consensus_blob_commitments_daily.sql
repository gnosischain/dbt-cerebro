

SELECT
    date
    ,total_blob_commitments AS value
FROM `dbt`.`int_consensus_blocks_daily`
ORDER BY date