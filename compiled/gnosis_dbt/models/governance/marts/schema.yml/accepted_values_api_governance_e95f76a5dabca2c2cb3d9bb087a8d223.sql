
    
    

with all_values as (

    select
        phase as value_field,
        count(*) as n_records

    from `dbt`.`api_governance_discussion_phases`
    group by phase

)

select *
from all_values
where value_field not in (
    'pre_discussion','pre_vote','voting','post_close'
)


