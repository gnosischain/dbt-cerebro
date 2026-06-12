





with validation_errors as (

    select
        week, avatar, earning_kind
    from (select * from `dbt`.`int_execution_circles_v2_economically_active_avatars_weekly` where toDate(week) >= today() - 7 - 7) dbt_subquery
    group by week, avatar, earning_kind
    having count(*) > 1

)

select *
from validation_errors


