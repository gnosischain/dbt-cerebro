
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`contracts_zodiac_modules_registry`
where address is not null
group by address
having count(*) > 1


