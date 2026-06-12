





with validation_errors as (

    select
        wallet_address, block_timestamp, transaction_hash, token_address, counterparty, direction
    from (select * from `dbt`.`int_execution_gpay_activity` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by wallet_address, block_timestamp, transaction_hash, token_address, counterparty, direction
    having count(*) > 1

)

select *
from validation_errors


