





with validation_errors as (

    select
        date, hour_of_day, day_of_week
    from (select * from `dbt`.`int_mixpanel_ga_usage_patterns_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, hour_of_day, day_of_week
    having count(*) > 1

)

select *
from validation_errors


