
    
    



select date
from (select * from `dbt`.`int_revenue_gpay_fees_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


