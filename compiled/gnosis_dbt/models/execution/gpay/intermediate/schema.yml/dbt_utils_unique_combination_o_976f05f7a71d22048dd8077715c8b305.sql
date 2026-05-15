





with validation_errors as (

    select
        conversion_date, conversion_kind, identity_role
    from `dbt`.`int_execution_gpay_coverage_daily`
    group by conversion_date, conversion_kind, identity_role
    having count(*) > 1

)

select *
from validation_errors


