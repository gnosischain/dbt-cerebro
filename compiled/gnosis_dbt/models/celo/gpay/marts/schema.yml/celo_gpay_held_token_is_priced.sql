



select
    1
from (select * from `dbt`.`fct_celo_gpay_balances_safe_daily` where abs(balance) > 0.000001) dbt_subquery

where not(balance_usd IS NOT NULL)

