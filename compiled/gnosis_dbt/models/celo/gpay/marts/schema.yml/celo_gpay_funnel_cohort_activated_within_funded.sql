



select
    1
from `dbt`.`fct_celo_gpay_funnel_cohorts_monthly`

where not(activated_7d <= funded_7d AND activated_30d <= funded_30d)

