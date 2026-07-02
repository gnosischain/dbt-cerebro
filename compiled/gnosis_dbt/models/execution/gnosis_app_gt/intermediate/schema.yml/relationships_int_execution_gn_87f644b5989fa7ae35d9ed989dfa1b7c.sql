
    
    

with child as (
    select circles_invited_by as from_field
    from `dbt`.`int_execution_gnosis_app_gt_user_dim`
    where circles_invited_by is not null
),

parent as (
    select address as to_field
    from `dbt`.`int_execution_gnosis_app_gt_user_dim`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


