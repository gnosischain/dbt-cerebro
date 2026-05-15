





with validation_errors as (

    select
        date
    from `dbt`.`int_execution_circles_v2_mints_daily`
    group by date
    having count(*) > 1

)

select *
from validation_errors


