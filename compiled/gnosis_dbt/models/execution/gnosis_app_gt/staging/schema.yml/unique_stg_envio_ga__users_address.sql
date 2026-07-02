
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`stg_envio_ga__users`
where address is not null
group by address
having count(*) > 1


