
    
    

select
    block_hash as unique_field,
    count(*) as n_records

from (select * from `dbt`.`stg_consensus__withdrawals` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where block_hash is not null
group by block_hash
having count(*) > 1


