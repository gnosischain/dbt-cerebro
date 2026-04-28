
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`api_execution_address_resolver`
where address is not null
group by address
having count(*) > 1


