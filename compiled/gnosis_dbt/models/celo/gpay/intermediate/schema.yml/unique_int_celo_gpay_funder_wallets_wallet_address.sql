
    
    

select
    wallet_address as unique_field,
    count(*) as n_records

from `dbt`.`int_celo_gpay_funder_wallets`
where wallet_address is not null
group by wallet_address
having count(*) > 1


