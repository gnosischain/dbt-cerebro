
    
    



select date
from (select * from `dbt`.`api_esg_estimated_nodes_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


