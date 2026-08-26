
    
    

select
    transaction_hash as unique_field,
    count(*) as n_records

from `dbt`.`fct_celo_gpay_settlement_batches`
where transaction_hash is not null
group by transaction_hash
having count(*) > 1


