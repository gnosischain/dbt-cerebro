





with validation_errors as (

    select
        date, event_name
    from `dbt`.`int_execution_circles_v2_hub_events_daily`
    group by date, event_name
    having count(*) > 1

)

select *
from validation_errors


