
    
    



select fees
from (select * from `dbt`.`int_revenue_sdai_fees_daily` where toDate(date) >= today() - 7) dbt_subquery
where fees is null


