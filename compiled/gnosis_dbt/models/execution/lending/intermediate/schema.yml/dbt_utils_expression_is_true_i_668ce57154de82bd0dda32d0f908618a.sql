



select
    1
from (select * from `dbt`.`int_execution_lending_aave_user_balances_daily` where toDate(date) >= today() - 7) dbt_subquery

where not(balance_usd > 0 OR balance <= 0)

