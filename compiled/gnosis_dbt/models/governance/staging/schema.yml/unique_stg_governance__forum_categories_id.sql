
    
    

select
    id as unique_field,
    count(*) as n_records

from `dbt`.`stg_governance__forum_categories`
where id is not null
group by id
having count(*) > 1


