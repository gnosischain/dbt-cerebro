





with validation_errors as (

    select
        block_timestamp, transaction_hash, trace_address
    from (select * from `dbt`.`contracts_circles_v2_Migration_calls` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, transaction_hash, trace_address
    having count(*) > 1

)

select *
from validation_errors


