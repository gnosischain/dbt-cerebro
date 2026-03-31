
    
    

select
    peer_id as unique_field,
    count(*) as n_records

from (select * from `dbt`.`stg_nebula_discv5__visits` where toDate(visit_started_at) >= today() - 7) dbt_subquery
where peer_id is not null
group by peer_id
having count(*) > 1


