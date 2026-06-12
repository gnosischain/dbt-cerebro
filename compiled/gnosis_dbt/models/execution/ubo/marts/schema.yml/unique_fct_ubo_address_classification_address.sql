
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`fct_ubo_address_classification`
where address is not null
group by address
having count(*) > 1


