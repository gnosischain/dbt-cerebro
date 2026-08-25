
    
    

with all_values as (

    select
        action as value_field,
        count(*) as n_records

    from `dbt`.`stg_governance__snapshot_delegations`
    group by action

)

select *
from all_values
where value_field not in (
    'set','clear'
)


