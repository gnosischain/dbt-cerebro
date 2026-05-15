





with validation_errors as (

    select
        week, address
    from `dbt`.`int_execution_circles_v2_gcrc_cashback_recipients_weekly`
    group by week, address
    having count(*) > 1

)

select *
from validation_errors


