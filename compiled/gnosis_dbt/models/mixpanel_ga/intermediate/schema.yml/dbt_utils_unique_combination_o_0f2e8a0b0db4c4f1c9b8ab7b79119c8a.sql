





with validation_errors as (

    select
        date, user_id_hash
    from (select * from `dbt`.`int_mixpanel_ga_users_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, user_id_hash
    having count(*) > 1

)

select *
from validation_errors


