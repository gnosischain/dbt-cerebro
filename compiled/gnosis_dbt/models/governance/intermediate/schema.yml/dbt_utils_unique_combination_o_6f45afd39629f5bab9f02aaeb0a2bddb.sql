





with validation_errors as (

    select
        chain_id, delegator
    from `dbt`.`int_governance_current_delegations`
    group by chain_id, delegator
    having count(*) > 1

)

select *
from validation_errors


