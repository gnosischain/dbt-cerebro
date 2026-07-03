
    
    

with all_values as (

    select
        period_type as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_gnosis_app_gt_active_wallets`
    group by period_type

)

select *
from all_values
where value_field not in (
    'day','week','month'
)


