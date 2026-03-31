
    
    



select month
from (select * from `dbt`.`fct_execution_gpay_actions_by_token_monthly` where toDate(month) >= today() - 7) dbt_subquery
where month is null


