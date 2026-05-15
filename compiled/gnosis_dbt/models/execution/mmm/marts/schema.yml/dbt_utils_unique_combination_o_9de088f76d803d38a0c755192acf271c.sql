





with validation_errors as (

    select
        week
    from `dbt`.`fct_execution_mmm_spine_weekly`
    group by week
    having count(*) > 1

)

select *
from validation_errors


