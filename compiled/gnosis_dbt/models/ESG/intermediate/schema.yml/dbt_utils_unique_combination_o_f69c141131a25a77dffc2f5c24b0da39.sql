





with validation_errors as (

    select
        month_date, country_code
    from `dbt`.`int_esg_carbon_intensity_ensemble`
    group by month_date, country_code
    having count(*) > 1

)

select *
from validation_errors


