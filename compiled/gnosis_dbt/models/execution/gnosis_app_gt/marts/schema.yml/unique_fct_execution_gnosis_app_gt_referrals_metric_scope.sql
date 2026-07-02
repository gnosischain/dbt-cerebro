
    
    

select
    metric_scope as unique_field,
    count(*) as n_records

from `dbt`.`fct_execution_gnosis_app_gt_referrals`
where metric_scope is not null
group by metric_scope
having count(*) > 1


