
    
    

select
    slot as unique_field,
    count(*) as n_records

from (select * from `dbt`.`stg_consensus__blocks` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where slot is not null
group by slot
having count(*) > 1


