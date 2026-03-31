
    
    



select month
from (select * from `dbt`.`api_execution_gpay_churn_monthly` where toDate(month) >= today() - 7) dbt_subquery
where month is null


