
    
    

with child as (
    select control_name as from_field
    from `dbt`.`int_execution_mmm_controls_weekly`
    where control_name is not null
),

parent as (
    select control_name as to_field
    from `dbt`.`mmm_control_registry`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


