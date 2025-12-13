
    
    

select
    crawl_id as unique_field,
    count(*) as n_records

from `dbt`.`stg_nebula_discv4__visits`
where crawl_id is not null
group by crawl_id
having count(*) > 1


