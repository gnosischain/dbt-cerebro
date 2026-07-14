
    
    

select
    account as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_gpay_accounts_deployed`
where account is not null
group by account
having count(*) > 1


