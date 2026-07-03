



select
    1
from `dbt`.`fct_execution_gnosis_app_gt_wallet_cohort_retention_monthly`

where not(retention_pct <= 1)

