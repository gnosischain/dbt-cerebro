



select
    1
from (select * from `dbt`.`int_execution_circles_v2_balances_daily` where account != '0x0000000000000000000000000000000000000000' AND date >= today() - 7) dbt_subquery

where not(balance_raw >= 0)

