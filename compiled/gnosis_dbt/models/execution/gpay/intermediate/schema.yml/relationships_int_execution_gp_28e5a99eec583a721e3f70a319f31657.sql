
    
    

with child as (
    select identity_role as from_field
    from `dbt`.`int_execution_gpay_user_identity_bridge`
    where identity_role is not null
),

parent as (
    select identity_role as to_field
    from `dbt`.`mta_gp_identity_roles`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


