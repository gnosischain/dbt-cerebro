
    
    



select peer_id
from (select * from `dbt`.`int_p2p_discv5_topology_latest` where toDate(date) >= today() - 7) dbt_subquery
where peer_id is null


