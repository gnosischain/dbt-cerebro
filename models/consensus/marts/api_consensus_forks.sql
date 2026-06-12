{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:forks_info', 'granularity:latest']
    )
}}

SELECT sub.*, today() AS as_of_date
FROM (
SELECT
  fork_name
  ,fork_version 
  ,fork_digest
  ,fork_epoch 
FROM {{ ref('fct_consensus_forks') }}
ORDER BY fork_version ASC
) AS sub
