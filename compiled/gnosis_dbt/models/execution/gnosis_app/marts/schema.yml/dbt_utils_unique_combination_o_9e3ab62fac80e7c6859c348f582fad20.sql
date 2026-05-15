





with validation_errors as (

    select
        date, funnel_name, user_pseudonym
    from `dbt`.`fct_execution_gnosis_app_funnel_daily`
    group by date, funnel_name, user_pseudonym
    having count(*) > 1

)

select *
from validation_errors


