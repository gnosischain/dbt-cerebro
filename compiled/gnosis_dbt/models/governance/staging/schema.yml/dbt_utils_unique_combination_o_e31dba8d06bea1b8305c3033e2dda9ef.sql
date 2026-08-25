





with validation_errors as (

    select
        chain_id, tx_hash, log_index
    from `dbt`.`stg_governance__snapshot_delegations`
    group by chain_id, tx_hash, log_index
    having count(*) > 1

)

select *
from validation_errors


