





with validation_errors as (

    select
        avatar
    from `dbt`.`int_execution_circles_v2_invite_funnel`
    group by avatar
    having count(*) > 1

)

select *
from validation_errors


