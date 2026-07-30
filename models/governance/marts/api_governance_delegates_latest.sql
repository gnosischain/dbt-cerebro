{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_delegates','granularity:latest']
  )
}}

-- Delegate leaderboard: how many gnosis.eth holders currently delegate to
-- each address, and since when. See int_governance_current_delegations for
-- how "current" is resolved.
SELECT sub.*, (SELECT toDate(max(delegated_at)) FROM {{ ref('int_governance_current_delegations') }}) AS as_of_date
FROM (
SELECT
    delegate,
    count()            AS delegator_count,
    min(delegated_at)  AS first_delegation_at,
    max(delegated_at)  AS last_delegation_at
FROM {{ ref('int_governance_current_delegations') }}
GROUP BY delegate
ORDER BY delegator_count DESC
) AS sub
