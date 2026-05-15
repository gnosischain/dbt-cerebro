





with validation_errors as (

    select
        week, media_name
    from `dbt`.`int_execution_mmm_media_weekly`
    group by week, media_name
    having count(*) > 1

)

select *
from validation_errors


