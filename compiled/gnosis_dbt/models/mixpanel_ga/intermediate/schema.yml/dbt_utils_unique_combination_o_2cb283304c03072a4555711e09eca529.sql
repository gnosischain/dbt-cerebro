





with validation_errors as (

    select
        date, country_code, region
    from (select * from `dbt`.`int_mixpanel_ga_geo_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, country_code, region
    having count(*) > 1

)

select *
from validation_errors


