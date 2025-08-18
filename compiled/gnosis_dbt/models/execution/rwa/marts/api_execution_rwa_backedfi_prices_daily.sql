SELECT
  bticker,
  date,
  price         
FROM `dbt`.`fct_execution_rwa_backedfi_prices_daily`
ORDER BY
  bticker,
  date