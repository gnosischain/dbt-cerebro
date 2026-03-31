
    
    



select date
from (select * from `dbt`.`api_p2p_topology_latest` where toDate(date) >= today() - 7) dbt_subquery
where date is null


