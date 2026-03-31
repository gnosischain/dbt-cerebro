
    
    



select date
from (select * from `dbt`.`api_esg_energy_monthly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


