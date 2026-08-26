



select
    1
from `dbt`.`fct_celo_gpay_funnel_cohorts_monthly`

where not(funded_7d <= eligible_7d AND funded_30d <= eligible_30d)

