





with validation_errors as (

    select
        date, symbol, user
    from (select * from `dbt`.`int_revenue_holdings_fees_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, symbol, user
    having count(*) > 1

)

select *
from validation_errors


