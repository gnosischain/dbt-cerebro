





with validation_errors as (

    select
        wallet_address, token
    from `dbt`.`fct_execution_gpay_user_balances_latest`
    group by wallet_address, token
    having count(*) > 1

)

select *
from validation_errors


