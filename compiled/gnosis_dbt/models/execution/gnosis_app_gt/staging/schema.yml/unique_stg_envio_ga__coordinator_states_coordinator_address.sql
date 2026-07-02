
    
    

select
    coordinator_address as unique_field,
    count(*) as n_records

from `dbt`.`stg_envio_ga__coordinator_states`
where coordinator_address is not null
group by coordinator_address
having count(*) > 1


