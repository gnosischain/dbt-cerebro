





with validation_errors as (

    select
        date, token_address
    from (select * from `dbt`.`int_execution_tokens_supply_holders_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, token_address
    having count(*) > 1

)

select *
from validation_errors


