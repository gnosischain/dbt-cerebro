





with validation_errors as (

    select
        backer
    from `dbt`.`int_execution_circles_v2_backers_current`
    group by backer
    having count(*) > 1

)

select *
from validation_errors


