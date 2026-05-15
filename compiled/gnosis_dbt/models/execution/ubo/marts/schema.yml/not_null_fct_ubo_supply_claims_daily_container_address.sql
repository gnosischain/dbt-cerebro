
    
    



select container_address
from (select * from `dbt`.`fct_ubo_supply_claims_daily` where toDate(date) >= today() - 7) dbt_subquery
where container_address is null


