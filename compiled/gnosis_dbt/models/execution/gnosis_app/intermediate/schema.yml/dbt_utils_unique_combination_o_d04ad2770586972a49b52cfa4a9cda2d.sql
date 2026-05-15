





with validation_errors as (

    select
        week, address
    from `dbt`.`int_execution_gnosis_app_weekly_earners`
    group by week, address
    having count(*) > 1

)

select *
from validation_errors


