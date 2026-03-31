
    
    



select month
from (select * from `dbt`.`api_execution_gpay_cashback_impact_monthly` where toDate(month) >= today() - 7) dbt_subquery
where month is null


