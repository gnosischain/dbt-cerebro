





with validation_errors as (

    select
        conversion_date, conversion_kind
    from (select * from `dbt`.`int_execution_gnosis_app_coverage_daily` where toDate(conversion_date) >= today() - 7) dbt_subquery
    group by conversion_date, conversion_kind
    having count(*) > 1

)

select *
from validation_errors


