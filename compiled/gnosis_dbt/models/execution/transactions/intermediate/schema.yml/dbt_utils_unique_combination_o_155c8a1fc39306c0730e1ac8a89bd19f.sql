





with validation_errors as (

    select
        date, transaction_type, success
    from (select * from `dbt`.`int_execution_transactions_info_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, transaction_type, success
    having count(*) > 1

)

select *
from validation_errors


