





with validation_errors as (

    select
        date
    from (select * from `dbt`.`int_execution_gnosis_app_swap_fees_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date
    having count(*) > 1

)

select *
from validation_errors


