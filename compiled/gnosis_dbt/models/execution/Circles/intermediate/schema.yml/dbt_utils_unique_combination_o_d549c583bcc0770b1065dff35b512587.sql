





with validation_errors as (

    select
        date, transfer_category
    from `dbt`.`int_execution_circles_v2_transfers_daily`
    group by date, transfer_category
    having count(*) > 1

)

select *
from validation_errors


