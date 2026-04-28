





with validation_errors as (

    select
        address, date
    from `dbt`.`fct_execution_account_balance_history_daily`
    group by address, date
    having count(*) > 1

)

select *
from validation_errors


