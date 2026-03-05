

SELECT
  window
  ,symbol
  ,from_label
  ,to_label
  ,amount_usd
  ,tf_cnt
FROM `dbt`.`fct_execution_gpay_flows_snapshot`
ORDER BY days ASC