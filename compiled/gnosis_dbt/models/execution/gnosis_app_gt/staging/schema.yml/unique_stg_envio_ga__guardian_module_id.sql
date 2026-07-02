
    
    

select
    id as unique_field,
    count(*) as n_records

from `dbt`.`stg_envio_ga__guardian_module`
where id is not null
group by id
having count(*) > 1


