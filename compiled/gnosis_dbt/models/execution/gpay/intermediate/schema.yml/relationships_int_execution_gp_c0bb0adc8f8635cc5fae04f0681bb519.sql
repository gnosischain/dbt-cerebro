
    
    

with child as (
    select event_kind as from_field
    from `dbt`.`int_execution_gpay_user_events_unified`
    where event_kind is not null
),

parent as (
    select event_kind as to_field
    from `dbt`.`mta_gp_event_kinds`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


