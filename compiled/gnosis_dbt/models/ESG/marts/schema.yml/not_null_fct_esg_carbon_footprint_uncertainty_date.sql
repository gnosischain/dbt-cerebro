
    
    



select date
from (select * from `dbt`.`fct_esg_carbon_footprint_uncertainty` where toDate(date) >= today() - 7) dbt_subquery
where date is null


