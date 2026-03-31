
    
    



select date
from (select * from `dbt`.`api_esg_carbon_timeseries_bands` where toDate(date) >= today() - 7) dbt_subquery
where date is null


