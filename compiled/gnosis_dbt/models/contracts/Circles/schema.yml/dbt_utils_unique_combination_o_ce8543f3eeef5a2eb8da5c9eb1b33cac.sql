





with validation_errors as (

    select
        block_timestamp, transaction_hash
    from (select * from `dbt`.`contracts_circles_v1_Hub_calls` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, transaction_hash
    having count(*) > 1

)

select *
from validation_errors


