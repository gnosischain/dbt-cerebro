
    
    



select date
from (select * from `dbt`.`int_ubo_claims_swapr_v3_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


