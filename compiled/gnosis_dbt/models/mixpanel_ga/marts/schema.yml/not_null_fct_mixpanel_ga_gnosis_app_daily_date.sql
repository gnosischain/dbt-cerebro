
    
    



select date
from (select * from `dbt`.`fct_mixpanel_ga_gnosis_app_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


