
    
    

with child as (
    select funnel_name as from_field
    from `dbt`.`fct_execution_gnosis_app_funnel_daily`
    where funnel_name is not null
),

parent as (
    select funnel_name as to_field
    from `dbt`.`mta_funnels`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


