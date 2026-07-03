





with validation_errors as (

    select
        address, activity_date
    from `dbt`.`stg_envio_ga__wallet_activity_daily`
    group by address, activity_date
    having count(*) > 1

)

select *
from validation_errors


