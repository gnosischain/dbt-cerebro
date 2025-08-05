
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`execution_state_address_current`
where address is not null
group by address
having count(*) > 1


