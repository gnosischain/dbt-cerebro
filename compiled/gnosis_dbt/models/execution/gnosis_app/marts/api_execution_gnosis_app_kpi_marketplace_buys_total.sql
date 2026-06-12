

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_gnosis_app_marketplace_payments`) AS as_of_date
FROM (
SELECT
    sum(total_buys)                                    AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM `dbt`.`fct_execution_gnosis_app_marketplace_offers_latest`
) AS sub