

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_gnosis_app_token_offer_claims`) AS as_of_date
FROM (
SELECT
    countDistinct(ga_user)                             AS value,
    CAST(NULL AS Nullable(Float64))                    AS change_pct
FROM `dbt`.`int_execution_gnosis_app_token_offer_claims`
WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
  AND toDate(block_timestamp) < today()
) AS sub