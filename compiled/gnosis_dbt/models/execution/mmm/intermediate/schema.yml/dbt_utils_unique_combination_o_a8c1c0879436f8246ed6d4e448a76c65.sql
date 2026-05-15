





with validation_errors as (

    select
        week, control_name
    from `dbt`.`int_execution_mmm_controls_weekly`
    group by week, control_name
    having count(*) > 1

)

select *
from validation_errors


