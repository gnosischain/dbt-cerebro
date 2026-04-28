
    
    



select date
from (select * from `dbt`.`fct_mixpanel_ga_gpay_crossdomain_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


