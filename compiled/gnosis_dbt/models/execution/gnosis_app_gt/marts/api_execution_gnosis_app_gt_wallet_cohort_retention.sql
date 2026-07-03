

-- Public acquisition-cohort retention matrix (retained wallets / cohort size at
-- month_index N). Point-in-time snapshot (as_of_date).
SELECT *, today() AS as_of_date
FROM `dbt`.`fct_execution_gnosis_app_gt_wallet_cohort_retention_monthly`
ORDER BY cohort_month DESC, month_index ASC