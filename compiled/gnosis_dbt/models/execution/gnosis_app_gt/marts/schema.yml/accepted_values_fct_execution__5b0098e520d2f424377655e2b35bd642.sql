
    
    

with all_values as (

    select
        metric_scope as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_gnosis_app_gt_referrals`
    group by metric_scope

)

select *
from all_values
where value_field not in (
    'earned','full_invite_graph'
)


