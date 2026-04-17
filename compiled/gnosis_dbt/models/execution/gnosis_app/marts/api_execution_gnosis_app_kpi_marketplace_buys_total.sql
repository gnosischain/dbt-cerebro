

SELECT
    sum(total_buys)                                    AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM `dbt`.`fct_execution_gnosis_app_marketplace_offers_latest`