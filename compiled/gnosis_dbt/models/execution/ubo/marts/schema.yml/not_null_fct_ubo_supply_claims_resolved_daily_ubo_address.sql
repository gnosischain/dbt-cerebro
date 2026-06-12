
    
    



select ubo_address
from (select * from `dbt`.`fct_ubo_supply_claims_resolved_daily` where toDate(date) >= today() - 7) dbt_subquery
where ubo_address is null


