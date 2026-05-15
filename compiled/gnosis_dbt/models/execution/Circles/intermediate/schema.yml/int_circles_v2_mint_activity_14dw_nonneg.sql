



select
    1
from (select * from `dbt`.`int_execution_circles_v2_mint_activity_daily` where date >= today() - 7) dbt_subquery

where not(mint_14dw >= 0)

