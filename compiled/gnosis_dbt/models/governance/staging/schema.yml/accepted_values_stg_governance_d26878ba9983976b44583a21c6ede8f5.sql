
    
    

with all_values as (

    select
        phase as value_field,
        count(*) as n_records

    from `dbt`.`stg_governance__forum_topics`
    group by phase

)

select *
from all_values
where value_field not in (
    'phase-1','phase-2','phase-3','none'
)


