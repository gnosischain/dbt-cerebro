





with validation_errors as (

    select
        basis, cohort_month, month_index
    from `dbt`.`fct_execution_gnosis_app_gt_wallet_cohort_retention_monthly`
    group by basis, cohort_month, month_index
    having count(*) > 1

)

select *
from validation_errors


