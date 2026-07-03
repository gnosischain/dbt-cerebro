





with validation_errors as (

    select
        month
    from `dbt`.`fct_execution_gnosis_app_gt_registrations_monthly`
    group by month
    having count(*) > 1

)

select *
from validation_errors


