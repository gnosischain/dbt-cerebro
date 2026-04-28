





with validation_errors as (

    select
        address, token_address
    from `dbt`.`fct_execution_account_token_balances_latest`
    group by address, token_address
    having count(*) > 1

)

select *
from validation_errors


