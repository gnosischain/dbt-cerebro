
    
    



select fee_native_sum
from `dbt`.`fct_execution_transactions_by_sector_daily`
where fee_native_sum is null


