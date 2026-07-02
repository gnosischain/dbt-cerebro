
    
    

select
    is_active as unique_field,
    count(*) as n_records

from `dbt`.`fct_execution_gnosis_app_gt_investments`
where is_active is not null
group by is_active
having count(*) > 1


