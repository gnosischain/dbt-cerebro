



select
    1
from (select * from `dbt`.`int_execution_circles_v2_tokens_supply_daily` where date >= today() - 7) dbt_subquery

where not(supply >= 0)

