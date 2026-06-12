





with validation_errors as (

    select
        block_timestamp, truster, trustee
    from (select * from `dbt`.`int_execution_circles_v2_trust_updates` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, truster, trustee
    having count(*) > 1

)

select *
from validation_errors


