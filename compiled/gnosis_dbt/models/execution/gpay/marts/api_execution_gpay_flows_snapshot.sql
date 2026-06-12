

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_transfers_whitelisted_daily`) AS as_of_date
FROM (
SELECT
  window
  ,symbol
  ,from_label
  ,to_label
  ,amount_usd
  ,tf_cnt
FROM `dbt`.`fct_execution_gpay_flows_snapshot`
ORDER BY days ASC
) AS sub