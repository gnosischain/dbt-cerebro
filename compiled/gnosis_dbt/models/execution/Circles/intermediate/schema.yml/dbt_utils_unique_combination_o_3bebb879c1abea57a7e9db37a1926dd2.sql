





with validation_errors as (

    select
        date, wrapper_address
    from `dbt`.`int_execution_circles_v2_wrapper_supply_daily`
    group by date, wrapper_address
    having count(*) > 1

)

select *
from validation_errors


