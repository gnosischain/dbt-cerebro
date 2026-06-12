





with validation_errors as (

    select
        address, date
    from (select * from `dbt`.`int_execution_account_balance_history_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by address, date
    having count(*) > 1

)

select *
from validation_errors


