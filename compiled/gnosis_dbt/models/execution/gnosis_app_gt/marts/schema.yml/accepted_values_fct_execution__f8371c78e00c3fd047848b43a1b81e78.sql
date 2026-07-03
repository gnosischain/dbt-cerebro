
    
    

with all_values as (

    select
        basis as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_gnosis_app_gt_wallet_cohort_retention_monthly`
    group by basis

)

select *
from all_values
where value_field not in (
    'any_action','app_tagged'
)


