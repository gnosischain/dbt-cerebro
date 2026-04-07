





with validation_errors as (

    select
        avatar, metadata_digest
    from `dbt`.`int_execution_circles_v2_avatar_metadata_history`
    group by avatar, metadata_digest
    having count(*) > 1

)

select *
from validation_errors


