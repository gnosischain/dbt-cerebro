
    
    

select
    id as unique_field,
    count(*) as n_records

from `dbt`.`stg_envio_ga__cashback_status_history`
where id is not null
group by id
having count(*) > 1


