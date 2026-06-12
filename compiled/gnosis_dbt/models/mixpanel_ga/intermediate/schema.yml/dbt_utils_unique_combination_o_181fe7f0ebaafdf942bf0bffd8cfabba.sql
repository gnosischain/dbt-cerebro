





with validation_errors as (

    select
        date, current_domain, page_path
    from (select * from `dbt`.`int_mixpanel_ga_pages_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, current_domain, page_path
    having count(*) > 1

)

select *
from validation_errors


