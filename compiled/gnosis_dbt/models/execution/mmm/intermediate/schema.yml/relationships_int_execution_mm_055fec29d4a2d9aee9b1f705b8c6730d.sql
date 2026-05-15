
    
    

with child as (
    select kpi_name as from_field
    from `dbt`.`int_execution_mmm_kpis_weekly`
    where kpi_name is not null
),

parent as (
    select kpi_name as to_field
    from `dbt`.`mmm_kpi_registry`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1


