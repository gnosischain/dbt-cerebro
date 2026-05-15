





with validation_errors as (

    select
        week, is_blacklisted
    from `dbt`.`fct_execution_gnosis_app_weekly_economically_active_users`
    group by week, is_blacklisted
    having count(*) > 1

)

select *
from validation_errors


