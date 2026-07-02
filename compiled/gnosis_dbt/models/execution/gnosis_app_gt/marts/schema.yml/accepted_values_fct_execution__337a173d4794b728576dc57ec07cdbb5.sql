
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_gnosis_app_gt_cashback_nft`
    group by status

)

select *
from all_values
where value_field not in (
    '0','1'
)


