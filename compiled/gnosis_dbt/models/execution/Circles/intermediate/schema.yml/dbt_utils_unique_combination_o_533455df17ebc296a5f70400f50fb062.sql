





with validation_errors as (

    select
        date, mint_kind
    from (select * from `dbt`.`int_execution_circles_v2_mints_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, mint_kind
    having count(*) > 1

)

select *
from validation_errors


