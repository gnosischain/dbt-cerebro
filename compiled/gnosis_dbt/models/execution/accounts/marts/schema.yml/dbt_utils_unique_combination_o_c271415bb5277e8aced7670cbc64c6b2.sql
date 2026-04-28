





with validation_errors as (

    select
        cohort_month
    from `dbt`.`fct_execution_network_retention_monthly`
    group by cohort_month
    having count(*) > 1

)

select *
from validation_errors


