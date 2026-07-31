





with validation_errors as (

    select
        date, symbol
    from (select * from `dbt`.`stg_crawlers_data__coingecko_prices` where toDate(date) >= today() - 7) dbt_subquery
    group by date, symbol
    having count(*) > 1

)

select *
from validation_errors


