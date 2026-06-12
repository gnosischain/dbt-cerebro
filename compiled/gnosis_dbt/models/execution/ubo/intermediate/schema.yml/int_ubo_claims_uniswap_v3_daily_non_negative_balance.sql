



select
    1
from (select * from `dbt`.`int_ubo_claims_uniswap_v3_daily` where toDate(date) >= today() - 7) dbt_subquery

where not(balance >= 0 AND balance_raw >= 0)

