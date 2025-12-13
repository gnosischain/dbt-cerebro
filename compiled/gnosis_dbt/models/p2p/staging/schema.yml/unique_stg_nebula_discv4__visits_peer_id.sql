
    
    

select
    peer_id as unique_field,
    count(*) as n_records

from `dbt`.`stg_nebula_discv4__visits`
where peer_id is not null
group by peer_id
having count(*) > 1


