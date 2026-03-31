
    
    

select
    peer_id as unique_field,
    count(*) as n_records

from (select * from `dbt`.`int_p2p_discv5_topology_latest` where toDate(date) >= today() - 7) dbt_subquery
where peer_id is not null
group by peer_id
having count(*) > 1


