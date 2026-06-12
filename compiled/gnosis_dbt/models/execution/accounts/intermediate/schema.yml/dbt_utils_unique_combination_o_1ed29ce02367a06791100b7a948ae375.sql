





with validation_errors as (

    select
        address, date, counterparty, token_address
    from (select * from `dbt`.`int_execution_account_token_movements_out_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by address, date, counterparty, token_address
    having count(*) > 1

)

select *
from validation_errors


