
    
    

select
    block_hash as unique_field,
    count(*) as n_records

from `dbt`.`stg_consensus__withdrawals`
where block_hash is not null
group by block_hash
having count(*) > 1


