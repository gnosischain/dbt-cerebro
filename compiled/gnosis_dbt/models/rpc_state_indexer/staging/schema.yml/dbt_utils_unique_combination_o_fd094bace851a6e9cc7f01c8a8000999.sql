





with validation_errors as (

    select
        chain_id, job_name, target_kind, target_address, snapshot_date
    from (select * from `dbt`.`stg_rpc_state_indexer__publications` where snapshot_date >= today() - 7) dbt_subquery
    group by chain_id, job_name, target_kind, target_address, snapshot_date
    having count(*) > 1

)

select *
from validation_errors


