

-- Delegate leaderboard: how many distinct gnosis.eth holders currently
-- delegate to each address across both DelegateRegistry chains. A holder
-- active on both chains counts once. See int_governance_current_delegations
-- for how "current" is resolved.
SELECT sub.*, (SELECT toDate(max(delegated_at)) FROM `dbt`.`int_governance_current_delegations`) AS as_of_date
FROM (
SELECT
    delegate,
    uniqExact(delegator) AS delegator_count,
    min(delegated_at)    AS first_delegation_at,
    max(delegated_at)    AS last_delegation_at
FROM `dbt`.`int_governance_current_delegations`
GROUP BY delegate
ORDER BY delegator_count DESC
) AS sub