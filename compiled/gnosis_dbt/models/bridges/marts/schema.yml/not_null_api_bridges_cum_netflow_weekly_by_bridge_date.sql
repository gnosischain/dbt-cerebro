
    
    



select date
from (select * from `dbt`.`api_bridges_cum_netflow_weekly_by_bridge` where toDate(date) >= today() - 7) dbt_subquery
where date is null


