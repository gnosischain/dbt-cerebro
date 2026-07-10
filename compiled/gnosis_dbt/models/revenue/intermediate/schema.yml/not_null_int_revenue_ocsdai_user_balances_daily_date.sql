
    
    



select date
from (select * from `dbt`.`int_revenue_ocsdai_user_balances_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


