



select
    1
from `dbt`.`fct_execution_mmm_baseline_latest`

where not(n_low_spend_weeks n_low_spend_weeks > 5)

