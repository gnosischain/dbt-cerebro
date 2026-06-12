



select
    1
from (select * from `dbt`.`fct_ubo_supply_claims_resolved_daily` where toDate(date) >= today() - 7) dbt_subquery

where not(container_address = lower(container_address) AND token_address = lower(token_address) AND ubo_address = lower(ubo_address))

