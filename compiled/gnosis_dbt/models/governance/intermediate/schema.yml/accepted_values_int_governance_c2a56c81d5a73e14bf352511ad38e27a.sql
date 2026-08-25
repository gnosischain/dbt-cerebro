
    
    

with all_values as (

    select
        outcome as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_proposals`
    group by outcome

)

select *
from all_values
where value_field not in (
    'passed','rejected','no_consensus','decided','below_quorum','open'
)


