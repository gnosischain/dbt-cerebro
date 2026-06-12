





with validation_errors as (

    select
        date, onboarding_class
    from `dbt`.`fct_execution_gnosis_app_gpay_volume_daily`
    group by date, onboarding_class
    having count(*) > 1

)

select *
from validation_errors


