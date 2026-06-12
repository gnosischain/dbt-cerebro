

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_lending_aave_daily`) AS as_of_date
FROM (
-- One row per protocol plus an ALL-protocols aggregate. See lenders_count_7d header.

SELECT
    token,
    protocol,
    value,
    change_pct
FROM `dbt`.`fct_execution_lending_latest`
WHERE label = 'Borrowers' AND window = '7D' AND token = 'ALL'
) AS sub