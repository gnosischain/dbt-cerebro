
    
    

with all_values as (

    select
        app_scope as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_gnosis_app_gt_swaps_summary`
    group by app_scope

)

select *
from all_values
where value_field not in (
    'gnosis_app','metri','third_party','unknown','test'
)


