
    
    

select
    group_address as unique_field,
    count(*) as n_records

from `dbt`.`api_execution_circles_v2_group_search`
where group_address is not null
group by group_address
having count(*) > 1


