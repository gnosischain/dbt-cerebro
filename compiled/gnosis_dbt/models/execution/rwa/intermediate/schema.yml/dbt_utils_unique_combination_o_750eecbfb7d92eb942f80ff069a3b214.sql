





with validation_errors as (

    select
        date, bticker
    from (select * from `dbt`.`int_execution_rwa_backedfi_prices` where toDate(date) >= today() - 7) dbt_subquery
    group by date, bticker
    having count(*) > 1

)

select *
from validation_errors


