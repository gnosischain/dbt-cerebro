SELECT
  fork_name
  ,fork_version 
  ,fork_digest
  ,fork_epoch 
FROM {{ ref('fct_consensus_forks') }}
ORDER BY fork_version ASC