
    
    

select
    account_address as unique_field,
    count(*) as n_records

from `dbt`.`stg_envio_ga__investment_accounts`
where account_address is not null
group by account_address
having count(*) > 1


