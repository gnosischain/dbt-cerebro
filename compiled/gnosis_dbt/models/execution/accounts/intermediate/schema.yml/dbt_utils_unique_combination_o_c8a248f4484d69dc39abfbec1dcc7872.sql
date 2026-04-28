





with validation_errors as (

    select
        date, address, counterparty, token_address
    from `dbt`.`int_execution_account_token_movements_in_daily`
    group by date, address, counterparty, token_address
    having count(*) > 1

)

select *
from validation_errors


