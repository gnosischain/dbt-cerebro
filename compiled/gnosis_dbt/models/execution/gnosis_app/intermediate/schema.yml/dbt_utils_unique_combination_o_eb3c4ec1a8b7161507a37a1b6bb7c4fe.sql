





with validation_errors as (

    select
        address, heuristic_kind, block_timestamp, transaction_hash
    from (select * from `dbt`.`int_execution_gnosis_app_user_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by address, heuristic_kind, block_timestamp, transaction_hash
    having count(*) > 1

)

select *
from validation_errors


