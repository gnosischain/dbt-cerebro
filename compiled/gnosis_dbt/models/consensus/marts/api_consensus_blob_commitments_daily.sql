

SELECT
    date
    ,cnt AS value
FROM `dbt`.`int_consensus_blob_commitments_daily`
ORDER BY date