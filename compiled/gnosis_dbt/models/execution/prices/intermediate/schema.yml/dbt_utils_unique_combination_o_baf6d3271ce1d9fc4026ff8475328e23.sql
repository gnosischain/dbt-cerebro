





with validation_errors as (

    select
        date, symbol, source
    from (select * from `dbt`.`int_execution_token_prices_external_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, symbol, source
    having count(*) > 1

)

select *
from validation_errors


