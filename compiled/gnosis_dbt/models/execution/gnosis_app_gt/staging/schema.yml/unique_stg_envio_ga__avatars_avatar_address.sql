
    
    

select
    avatar_address as unique_field,
    count(*) as n_records

from `dbt`.`stg_envio_ga__avatars`
where avatar_address is not null
group by avatar_address
having count(*) > 1


