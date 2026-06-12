





with validation_errors as (

    select
        date, token_address, "from", "to"
    from (select * from `dbt`.`int_execution_transfers_whitelisted_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, token_address, "from", "to"
    having count(*) > 1

)

select *
from validation_errors


