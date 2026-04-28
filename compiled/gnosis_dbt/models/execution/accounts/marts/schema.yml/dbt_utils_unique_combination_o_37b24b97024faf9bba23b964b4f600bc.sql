





with validation_errors as (

    select
        date, address, counterparty, token_address, direction
    from `dbt`.`fct_execution_account_token_movements_daily`
    group by date, address, counterparty, token_address, direction
    having count(*) > 1

)

select *
from validation_errors


