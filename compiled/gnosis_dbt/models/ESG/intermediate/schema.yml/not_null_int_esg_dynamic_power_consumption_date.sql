
    
    



select date
from (select * from `dbt`.`int_esg_dynamic_power_consumption` where toDate(date) >= today() - 7) dbt_subquery
where date is null


