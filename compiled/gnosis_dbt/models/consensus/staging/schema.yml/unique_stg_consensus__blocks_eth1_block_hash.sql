
    
    

select
    eth1_block_hash as unique_field,
    count(*) as n_records

from `dbt`.`stg_consensus__blocks`
where eth1_block_hash is not null
group by eth1_block_hash
having count(*) > 1


