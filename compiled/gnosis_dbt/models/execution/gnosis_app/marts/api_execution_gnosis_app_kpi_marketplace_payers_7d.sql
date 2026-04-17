

SELECT
    countDistinct(payer)                             AS value,
    CAST(NULL AS Nullable(Float64))                  AS change_pct
FROM `dbt`.`int_execution_gnosis_app_marketplace_payments`
WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
  AND toDate(block_timestamp) < today()