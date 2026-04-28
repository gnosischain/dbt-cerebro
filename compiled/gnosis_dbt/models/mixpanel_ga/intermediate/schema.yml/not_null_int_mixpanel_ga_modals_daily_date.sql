
    
    



select date
from (select * from `dbt`.`int_mixpanel_ga_modals_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


