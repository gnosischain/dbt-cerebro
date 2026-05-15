
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`stg_crawlers_data__circles_blacklisted`
where address is not null
group by address
having count(*) > 1


