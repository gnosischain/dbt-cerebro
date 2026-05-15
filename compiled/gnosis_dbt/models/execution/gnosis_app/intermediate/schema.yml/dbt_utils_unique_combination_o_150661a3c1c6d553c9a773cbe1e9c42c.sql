





with validation_errors as (

    select
        conversion_date, conversion_kind
    from `dbt`.`int_execution_gnosis_app_coverage_daily`
    group by conversion_date, conversion_kind
    having count(*) > 1

)

select *
from validation_errors


