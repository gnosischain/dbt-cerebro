





with validation_errors as (

    select
        month, cohort
    from `dbt`.`fct_revenue_gnosis_app_cohorts_monthly`
    group by month, cohort
    having count(*) > 1

)

select *
from validation_errors


