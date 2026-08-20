{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_delegation_graph','granularity:latest']
  )
}}

-- Current delegator -> delegate edges (one row per active delegation per
-- chain), for a delegation-network visual. chain_id keeps the two
-- registries separable; tx_hash lets a reader verify any edge on-chain.
-- See int_governance_current_delegations for resolution.
SELECT sub.*, (SELECT toDate(max(delegated_at)) FROM {{ ref('int_governance_current_delegations') }}) AS as_of_date
FROM (
SELECT
    chain_id,
    delegator,
    delegate,
    delegated_at,
    tx_hash
FROM {{ ref('int_governance_current_delegations') }}
ORDER BY delegated_at DESC
) AS sub
