
    
    

select
    contract_address as unique_field,
    count(*) as n_records

from `dbt`.`contracts_abi`
where contract_address is not null
group by contract_address
having count(*) > 1


