





with validation_errors as (

    select
        country_code, month_date
    from `dbt`.`int_quarterly_esg_carbon_intensity_with_fallback`
    group by country_code, month_date
    having count(*) > 1

)

select *
from validation_errors


