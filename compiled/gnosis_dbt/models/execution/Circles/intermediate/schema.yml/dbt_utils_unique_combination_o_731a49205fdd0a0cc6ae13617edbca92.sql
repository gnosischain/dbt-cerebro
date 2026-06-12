





with validation_errors as (

    select
        date, account, token_address
    from (select * from `dbt`.`int_execution_circles_v2_balance_diffs_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, account, token_address
    having count(*) > 1

)

select *
from validation_errors


