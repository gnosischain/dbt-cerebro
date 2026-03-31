
    
    

select
    eth1_block_hash as unique_field,
    count(*) as n_records

from (select * from `dbt`.`stg_consensus__blocks` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where eth1_block_hash is not null
group by eth1_block_hash
having count(*) > 1


