
    
    

with all_values as (

    select
        state as value_field,
        count(*) as n_records

    from `dbt`.`stg_governance__snapshot_proposals`
    group by state

)

select *
from all_values
where value_field not in (
    'pending','active','closed'
)


