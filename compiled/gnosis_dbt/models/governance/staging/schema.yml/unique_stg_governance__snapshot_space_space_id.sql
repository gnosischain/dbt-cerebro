
    
    

select
    space_id as unique_field,
    count(*) as n_records

from `dbt`.`stg_governance__snapshot_space`
where space_id is not null
group by space_id
having count(*) > 1


