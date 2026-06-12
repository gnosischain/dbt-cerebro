



select
    1
from (select * from `dbt`.`fct_ubo_supply_claims_resolved_daily` where toDate(date) >= today() - 7) dbt_subquery

where not(balance >= 0 AND balance_raw >= 0)

