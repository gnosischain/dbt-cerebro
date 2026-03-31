
    
    



select date
from (select * from `dbt`.`api_execution_gpay_funded_addresses_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


