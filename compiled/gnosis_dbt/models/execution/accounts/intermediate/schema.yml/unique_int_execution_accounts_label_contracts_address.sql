
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_accounts_label_contracts`
where address is not null
group by address
having count(*) > 1


