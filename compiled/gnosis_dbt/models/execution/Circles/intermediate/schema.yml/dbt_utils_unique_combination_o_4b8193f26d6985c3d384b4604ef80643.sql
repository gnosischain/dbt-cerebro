





with validation_errors as (

    select
        token_address, date
    from (select * from `dbt`.`int_execution_circles_v2_tokens_supply_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by token_address, date
    having count(*) > 1

)

select *
from validation_errors


