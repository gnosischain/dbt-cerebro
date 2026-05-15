
    
    

with child as (
    select media_name as from_field
    from `dbt`.`int_execution_mmm_media_weekly`
    where media_name is not null
),

parent as (
    select media_name as to_field
    from `dbt`.`mmm_media_registry`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


