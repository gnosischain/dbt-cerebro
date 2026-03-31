
    
    



select date
from (select * from `dbt`.`api_execution_gpay_volume_payments_by_token_weekly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


