
    
    



select date
from (select * from `dbt`.`api_execution_circles_v2_crc20_prices_daily` where date >= today() - 7) dbt_subquery
where date is null


