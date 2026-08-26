



select
    1
from (select * from `dbt`.`fct_celo_gpay_balances_safe_daily` where abs(balance) > 0.000001 AND date >= today() - 3) dbt_subquery

where not(dateDiff('day', price_date, date) <= 2)

