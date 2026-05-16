





with validation_errors as (

    select
        transaction_hash, log_index, batch_index
    from `dbt`.`int_execution_circles_v2_mint_events`
    group by transaction_hash, log_index, batch_index
    having count(*) > 1

)

select *
from validation_errors


