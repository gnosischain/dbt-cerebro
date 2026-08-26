

-- Cards that have ever SPENT, cumulative. See api_celo_gpay_activated_addresses_daily
-- for why this series exists separately from the funded one.
SELECT
    month                AS date,
    cumulative_activated AS value
FROM `dbt`.`fct_celo_gpay_activity_monthly`
ORDER BY date