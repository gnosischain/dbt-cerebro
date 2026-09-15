
    
    



select date
from (select * from `dbt`.`int_rpc_state_indexer_token_balances_priced_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


