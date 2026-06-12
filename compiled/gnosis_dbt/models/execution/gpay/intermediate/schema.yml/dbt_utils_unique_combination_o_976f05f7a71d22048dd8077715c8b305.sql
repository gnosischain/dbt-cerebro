





with validation_errors as (

    select
        conversion_date, conversion_kind, identity_role
    from (select * from `dbt`.`int_execution_gpay_coverage_daily` where toDate(conversion_date) >= today() - 7) dbt_subquery
    group by conversion_date, conversion_kind, identity_role
    having count(*) > 1

)

select *
from validation_errors


