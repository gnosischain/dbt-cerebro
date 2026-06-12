





with validation_errors as (

    select
        date, transfer_category
    from (select * from `dbt`.`int_execution_circles_v2_transfers_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, transfer_category
    having count(*) > 1

)

select *
from validation_errors


