





with validation_errors as (

    select
        date, referrer_domain, initial_referrer_domain
    from (select * from `dbt`.`int_mixpanel_ga_traffic_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, referrer_domain, initial_referrer_domain
    having count(*) > 1

)

select *
from validation_errors


