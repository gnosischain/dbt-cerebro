
    
    



select date
from (select * from `dbt`.`int_mixpanel_ga_pages_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


