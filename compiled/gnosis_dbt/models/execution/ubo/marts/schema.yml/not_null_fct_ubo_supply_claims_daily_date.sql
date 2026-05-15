
    
    



select date
from (select * from `dbt`.`fct_ubo_supply_claims_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


