





with validation_errors as (

    select
        date, wallet_address, action, symbol
    from (select * from `dbt`.`int_execution_gpay_activity_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, wallet_address, action, symbol
    having count(*) > 1

)

select *
from validation_errors


