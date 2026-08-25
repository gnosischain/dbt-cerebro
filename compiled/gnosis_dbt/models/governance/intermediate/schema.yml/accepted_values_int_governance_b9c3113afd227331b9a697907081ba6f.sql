
    
    

with all_values as (

    select
        metric as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_engagement_counters_daily`
    group by metric

)

select *
from all_values
where value_field not in (
    'views','reads'
)


