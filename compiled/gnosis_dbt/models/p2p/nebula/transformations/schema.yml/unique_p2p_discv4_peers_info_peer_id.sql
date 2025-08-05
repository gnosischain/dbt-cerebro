
    
    

select
    peer_id as unique_field,
    count(*) as n_records

from `dbt`.`p2p_discv4_peers_info`
where peer_id is not null
group by peer_id
having count(*) > 1


