
    
    

select
    pool_address as unique_field,
    count(*) as n_records

from `dbt`.`api_execution_circles_v2_pools_latest`
where pool_address is not null
group by pool_address
having count(*) > 1


