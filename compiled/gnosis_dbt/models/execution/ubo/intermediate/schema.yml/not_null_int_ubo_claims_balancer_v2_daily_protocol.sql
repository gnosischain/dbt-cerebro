
    
    



select protocol
from (select * from `dbt`.`int_ubo_claims_balancer_v2_daily` where toDate(date) >= today() - 7) dbt_subquery
where protocol is null


