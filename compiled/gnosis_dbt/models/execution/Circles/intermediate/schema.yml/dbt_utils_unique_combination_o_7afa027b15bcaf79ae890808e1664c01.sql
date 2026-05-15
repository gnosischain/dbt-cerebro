





with validation_errors as (

    select
        week, address
    from `dbt`.`int_execution_circles_v2_active_avatars_weekly`
    group by week, address
    having count(*) > 1

)

select *
from validation_errors


