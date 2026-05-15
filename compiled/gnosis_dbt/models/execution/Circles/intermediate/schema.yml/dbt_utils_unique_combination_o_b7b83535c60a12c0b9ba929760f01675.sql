





with validation_errors as (

    select
        avatar, date
    from `dbt`.`int_execution_circles_v2_mint_activity_daily`
    group by avatar, date
    having count(*) > 1

)

select *
from validation_errors


