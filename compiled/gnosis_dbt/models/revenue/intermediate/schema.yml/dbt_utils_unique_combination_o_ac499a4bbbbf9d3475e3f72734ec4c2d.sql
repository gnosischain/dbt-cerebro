





with validation_errors as (

    select
        date, user
    from (select * from `dbt`.`int_revenue_ocsdai_user_balances_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, user
    having count(*) > 1

)

select *
from validation_errors


