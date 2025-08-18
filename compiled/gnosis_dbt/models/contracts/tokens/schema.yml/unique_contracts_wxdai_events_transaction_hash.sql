
    
    

select
    transaction_hash as unique_field,
    count(*) as n_records

from `dbt`.`contracts_wxdai_events`
where transaction_hash is not null
group by transaction_hash
having count(*) > 1


