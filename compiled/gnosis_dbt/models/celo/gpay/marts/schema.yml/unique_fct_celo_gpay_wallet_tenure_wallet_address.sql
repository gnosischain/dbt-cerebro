
    
    

select
    wallet_address as unique_field,
    count(*) as n_records

from `dbt`.`fct_celo_gpay_wallet_tenure`
where wallet_address is not null
group by wallet_address
having count(*) > 1


