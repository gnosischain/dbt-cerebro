
    
    

with all_values as (

    select
        entity_kind as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_engagement_counters_daily`
    group by entity_kind

)

select *
from all_values
where value_field not in (
    'topic','post'
)


