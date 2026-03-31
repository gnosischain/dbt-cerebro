
    
    



select date
from (select * from `dbt`.`api_execution_blocks_gas_usage_pct_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


