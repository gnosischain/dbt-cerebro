
    
    

select
    block_hash as unique_field,
    count(*) as n_records

from `dbt`.`stg_execution__logs`
where block_hash is not null
group by block_hash
having count(*) > 1


