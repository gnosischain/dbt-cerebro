
    
    

with all_values as (

    select
        app_generation as value_field,
        count(*) as n_records

    from `dbt`.`int_execution_gnosis_app_gt_user_activity`
    group by app_generation

)

select *
from all_values
where value_field not in (
    'current','legacy','both','none'
)


