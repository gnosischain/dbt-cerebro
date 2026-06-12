
    
    

select
    old_safe as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_gpay_safe_switchover`
where old_safe is not null
group by old_safe
having count(*) > 1


