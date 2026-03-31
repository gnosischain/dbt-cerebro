
    
    



select date
from (select * from `dbt`.`api_probelab_clients_cloud_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


