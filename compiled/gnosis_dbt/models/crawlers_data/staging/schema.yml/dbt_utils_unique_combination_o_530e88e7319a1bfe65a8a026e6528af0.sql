





with validation_errors as (

    select
        date, symbol
    from `dbt`.`stg_crawlers_data__dune_prices`
    group by date, symbol
    having count(*) > 1

)

select *
from validation_errors


