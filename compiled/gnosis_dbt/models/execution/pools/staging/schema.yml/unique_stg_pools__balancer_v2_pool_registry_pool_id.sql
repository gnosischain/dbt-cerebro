
    
    

select
    pool_id as unique_field,
    count(*) as n_records

from `dbt`.`stg_pools__balancer_v2_pool_registry`
where pool_id is not null
group by pool_id
having count(*) > 1


