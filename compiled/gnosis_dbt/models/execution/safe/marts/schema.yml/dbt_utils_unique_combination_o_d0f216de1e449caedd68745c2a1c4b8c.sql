





with validation_errors as (

    select
        owner_address, safe_address
    from `dbt`.`fct_execution_account_safes_latest`
    group by owner_address, safe_address
    having count(*) > 1

)

select *
from validation_errors


