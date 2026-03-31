
    
    



select dest_chain
from (select * from `dbt`.`int_bridges_flows_daily` where toDate(date) >= today() - 7) dbt_subquery
where dest_chain is null


