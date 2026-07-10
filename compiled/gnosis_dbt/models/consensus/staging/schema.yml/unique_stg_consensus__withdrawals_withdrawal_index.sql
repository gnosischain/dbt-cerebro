
    
    

select
    withdrawal_index as unique_field,
    count(*) as n_records

from (select * from `dbt`.`stg_consensus__withdrawals` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where withdrawal_index is not null
group by withdrawal_index
having count(*) > 1


