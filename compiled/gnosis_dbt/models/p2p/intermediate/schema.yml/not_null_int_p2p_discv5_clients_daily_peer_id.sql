
    
    



select peer_id
from (select * from `dbt`.`int_p2p_discv5_clients_daily` where toDate(date) >= today() - 7) dbt_subquery
where peer_id is null


