





with validation_errors as (

    select
        proposal_id, voter
    from `dbt`.`stg_governance__snapshot_votes`
    group by proposal_id, voter
    having count(*) > 1

)

select *
from validation_errors


