
    
    

select
    id as unique_field,
    count(*) as n_records

from `dbt`.`contracts_circles_v2_NameRegistry_events`
where id is not null
group by id
having count(*) > 1


