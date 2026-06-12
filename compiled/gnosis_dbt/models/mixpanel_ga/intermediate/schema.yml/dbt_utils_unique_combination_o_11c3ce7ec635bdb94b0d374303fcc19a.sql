





with validation_errors as (

    select
        date, bottom_sheet
    from (select * from `dbt`.`int_mixpanel_ga_modals_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, bottom_sheet
    having count(*) > 1

)

select *
from validation_errors


