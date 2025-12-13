
    
    

select
    transaction_hash as unique_field,
    count(*) as n_records

from `dbt`.`stg_execution__transactions`
where transaction_hash is not null
group by transaction_hash
having count(*) > 1


