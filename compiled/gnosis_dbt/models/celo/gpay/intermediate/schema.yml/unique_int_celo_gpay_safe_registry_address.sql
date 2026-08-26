
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`int_celo_gpay_safe_registry`
where address is not null
group by address
having count(*) > 1


