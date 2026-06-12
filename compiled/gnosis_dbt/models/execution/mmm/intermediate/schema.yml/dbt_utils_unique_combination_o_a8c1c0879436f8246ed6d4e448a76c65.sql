





with validation_errors as (

    select
        week, control_name
    from (select * from `dbt`.`int_execution_mmm_controls_weekly` where toDate(week) >= today() - 7) dbt_subquery
    group by week, control_name
    having count(*) > 1

)

select *
from validation_errors


