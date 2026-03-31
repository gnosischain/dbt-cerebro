
    
    



select date
from (select * from `dbt`.`api_execution_gpay_cashback_recipients_weekly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


