





with validation_errors as (

    select
        block_timestamp, log_index
    from (select * from `dbt`.`contracts_backedfi_bCOIN_Oracle_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, log_index
    having count(*) > 1

)

select *
from validation_errors


