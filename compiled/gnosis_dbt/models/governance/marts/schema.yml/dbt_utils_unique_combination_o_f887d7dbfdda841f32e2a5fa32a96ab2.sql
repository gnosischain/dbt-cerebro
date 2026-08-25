





with validation_errors as (

    select
        chain_id, delegator
    from `dbt`.`api_governance_delegation_graph`
    group by chain_id, delegator
    having count(*) > 1

)

select *
from validation_errors


