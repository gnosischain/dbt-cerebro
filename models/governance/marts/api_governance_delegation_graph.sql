{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_delegation_graph','granularity:latest']
  )
}}

-- Current delegator -> delegate edges (one row per active delegation), for
-- a delegation-network visual. tx_hash lets a reader verify any edge
-- directly on-chain. See int_governance_current_delegations for resolution.
SELECT
    delegator,
    delegate,
    delegated_at,
    tx_hash
FROM {{ ref('int_governance_current_delegations') }}
ORDER BY delegated_at DESC
