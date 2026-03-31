
    
    



select date
from (select * from `dbt`.`api_execution_gpay_payments_by_token_monthly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


