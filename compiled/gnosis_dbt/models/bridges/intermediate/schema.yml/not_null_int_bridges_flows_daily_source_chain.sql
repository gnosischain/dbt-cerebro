
    
    



select source_chain
from (select * from `dbt`.`int_bridges_flows_daily` where toDate(date) >= today() - 7) dbt_subquery
where source_chain is null


