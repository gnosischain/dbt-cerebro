
    
    



select date
from (select * from `dbt`.`int_ubo_claims_sdai_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


