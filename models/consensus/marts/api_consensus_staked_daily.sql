
SELECT
    date
    ,effective_balance AS value
FROM {{ ref('int_consensus_validators_balances_daily') }}
ORDER BY date