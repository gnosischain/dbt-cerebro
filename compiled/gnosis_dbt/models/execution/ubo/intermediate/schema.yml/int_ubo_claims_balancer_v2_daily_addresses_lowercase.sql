



select
    1
from (select * from `dbt`.`int_ubo_claims_balancer_v2_daily` where toDate(date) >= today() - 7) dbt_subquery

where not(container_address = lower(container_address) AND token_address = lower(token_address) AND ubo_address = lower(ubo_address))

