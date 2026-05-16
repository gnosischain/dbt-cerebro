





with validation_errors as (

    select
        date, mint_kind
    from `dbt`.`int_execution_circles_v2_mints_daily`
    group by date, mint_kind
    having count(*) > 1

)

select *
from validation_errors


