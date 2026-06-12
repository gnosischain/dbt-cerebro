





with validation_errors as (

    select
        block_timestamp, transaction_hash
    from (select * from `dbt`.`int_execution_pools_dex_trades_tx_context` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, transaction_hash
    having count(*) > 1

)

select *
from validation_errors


