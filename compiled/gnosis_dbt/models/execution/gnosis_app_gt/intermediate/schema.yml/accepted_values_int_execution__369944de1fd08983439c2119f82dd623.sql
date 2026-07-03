
    
    

with all_values as (

    select
        engagement_tier as value_field,
        count(*) as n_records

    from `dbt`.`int_execution_gnosis_app_gt_wallet_metrics`
    group by engagement_tier

)

select *
from all_values
where value_field not in (
    'inactive','casual','core','power'
)


