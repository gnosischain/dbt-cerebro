





with validation_errors as (

    select
        area, month_date, electricity_source
    from `dbt`.`stg_crawlers_data__ember_generation_monthly`
    group by area, month_date, electricity_source
    having count(*) > 1

)

select *
from validation_errors


