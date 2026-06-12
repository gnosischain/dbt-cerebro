





with validation_errors as (

    select
        month_date, country_code
    from (select * from `dbt`.`int_esg_carbon_intensity_ensemble` where toDate(month_date) >= today() - 7) dbt_subquery
    group by month_date, country_code
    having count(*) > 1

)

select *
from validation_errors


