





with validation_errors as (

    select
        date, gp_safe
    from (select * from `dbt`.`int_execution_gpay_delay_activity_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, gp_safe
    having count(*) > 1

)

select *
from validation_errors


