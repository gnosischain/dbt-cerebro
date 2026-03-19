

WITH address_first_seen AS (
    SELECT
        address_hash,
        min(first_seen_date) AS first_seen_date
    FROM `dbt`.`int_execution_transactions_unique_addresses`
    GROUP BY address_hash
)

SELECT
    first_seen_date                                                     AS date,
    toUInt64(count())                                                   AS new_accounts,
    toUInt64(sum(count()) OVER (ORDER BY first_seen_date))              AS cumulative_accounts
FROM address_first_seen
GROUP BY first_seen_date
ORDER BY first_seen_date