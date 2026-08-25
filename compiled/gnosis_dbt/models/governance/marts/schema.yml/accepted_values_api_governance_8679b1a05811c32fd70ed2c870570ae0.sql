
    
    

with all_values as (

    select
        poll_phase as value_field,
        count(*) as n_records

    from `dbt`.`api_governance_poll_vs_vote`
    group by poll_phase

)

select *
from all_values
where value_field not in (
    'pre_discussion','pre_vote','voting','post_close'
)


