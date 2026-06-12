

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_consensus_deposits_withdrawals_daily`) AS as_of_date
FROM (
SELECT
    value
    ,change_pct
FROM 
    `dbt`.`fct_consensus_info_latest`
WHERE
    label = 'withdrawls_cnt'
) AS sub