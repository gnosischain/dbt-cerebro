

SELECT
  fork_name
  ,fork_version 
  ,fork_digest
  ,fork_epoch 
FROM `dbt`.`fct_consensus_forks`
ORDER BY fork_version ASC