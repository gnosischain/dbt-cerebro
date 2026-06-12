





with validation_errors as (

    select
        date, browser, os, device_type
    from (select * from `dbt`.`int_mixpanel_ga_tech_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, browser, os, device_type
    having count(*) > 1

)

select *
from validation_errors


