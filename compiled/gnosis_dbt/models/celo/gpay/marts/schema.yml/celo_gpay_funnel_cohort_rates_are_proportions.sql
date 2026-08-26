



select
    1
from (select * from `dbt`.`fct_celo_gpay_funnel_cohorts_monthly` where funded_rate_7d IS NOT NULL AND funded_rate_30d IS NOT NULL) dbt_subquery

where not(funded_rate_7d BETWEEN 0 AND 1 AND funded_rate_30d BETWEEN 0 AND 1)

