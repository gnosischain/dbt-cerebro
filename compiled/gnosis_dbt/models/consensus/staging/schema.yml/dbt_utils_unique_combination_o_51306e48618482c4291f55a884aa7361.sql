





with validation_errors as (

    select
        slot, validator_index
    from (select * from `dbt`.`stg_consensus__validators` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
    group by slot, validator_index
    having count(*) > 1

)

select *
from validation_errors


