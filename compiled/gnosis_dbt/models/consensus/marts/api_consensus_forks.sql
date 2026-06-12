

SELECT sub.*, today() AS as_of_date
FROM (
SELECT
  fork_name
  ,fork_version 
  ,fork_digest
  ,fork_epoch 
FROM `dbt`.`fct_consensus_forks`
ORDER BY fork_version ASC
) AS sub