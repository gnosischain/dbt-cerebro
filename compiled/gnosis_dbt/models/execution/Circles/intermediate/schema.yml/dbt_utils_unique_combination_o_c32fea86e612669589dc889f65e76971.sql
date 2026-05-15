





with validation_errors as (

    select
        date, lifecycle_stage
    from `dbt`.`int_execution_circles_v2_backing_events_daily`
    group by date, lifecycle_stage
    having count(*) > 1

)

select *
from validation_errors


