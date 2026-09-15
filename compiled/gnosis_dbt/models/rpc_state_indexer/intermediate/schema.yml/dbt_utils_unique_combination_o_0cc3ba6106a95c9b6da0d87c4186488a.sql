





with validation_errors as (

    select
        date, token_address, address
    from (select * from `dbt`.`int_rpc_state_indexer_token_balances_priced_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, token_address, address
    having count(*) > 1

)

select *
from validation_errors


