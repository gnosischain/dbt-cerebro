{{ config(materialized='view') }}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    e.event_name,
    lower(decoded_params['solver'])                                          AS solver
FROM {{ ref('contracts_CowProtocol_GPv2AllowListAuthentication_events') }} e
WHERE e.event_name IN ('SolverAdded', 'SolverRemoved')
  AND decoded_params['solver'] IS NOT NULL
