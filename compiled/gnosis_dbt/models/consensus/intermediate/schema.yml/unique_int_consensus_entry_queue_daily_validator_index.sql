
    
    

select
    validator_index as unique_field,
    count(*) as n_records

from `dbt`.`int_consensus_entry_queue_daily`
where validator_index is not null
group by validator_index
having count(*) > 1


