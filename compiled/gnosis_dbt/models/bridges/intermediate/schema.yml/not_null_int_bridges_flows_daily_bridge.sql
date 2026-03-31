
    
    



select bridge
from (select * from `dbt`.`int_bridges_flows_daily` where toDate(date) >= today() - 7) dbt_subquery
where bridge is null


