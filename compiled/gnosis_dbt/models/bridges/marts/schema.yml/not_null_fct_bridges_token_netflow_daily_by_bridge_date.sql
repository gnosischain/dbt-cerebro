
    
    



select date
from (select * from `dbt`.`fct_bridges_token_netflow_daily_by_bridge` where toDate(date) >= today() - 7) dbt_subquery
where date is null


