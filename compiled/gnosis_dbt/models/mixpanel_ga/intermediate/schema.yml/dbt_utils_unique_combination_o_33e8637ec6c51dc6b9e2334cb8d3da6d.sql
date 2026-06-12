





with validation_errors as (

    select
        date, event_name, event_category
    from (select * from `dbt`.`int_mixpanel_ga_events_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, event_name, event_category
    having count(*) > 1

)

select *
from validation_errors


