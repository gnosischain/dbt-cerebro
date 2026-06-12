





with validation_errors as (

    select
        address, date, counterparty, token_address, direction
    from (select * from `dbt`.`fct_execution_account_token_movements_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by address, date, counterparty, token_address, direction
    having count(*) > 1

)

select *
from validation_errors


