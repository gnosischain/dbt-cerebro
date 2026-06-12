





with validation_errors as (

    select
        date, lifecycle_stage
    from (select * from `dbt`.`int_execution_circles_v2_backing_events_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, lifecycle_stage
    having count(*) > 1

)

select *
from validation_errors


