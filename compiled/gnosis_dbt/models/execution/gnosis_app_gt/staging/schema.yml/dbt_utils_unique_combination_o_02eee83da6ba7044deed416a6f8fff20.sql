





with validation_errors as (

    select
        participant, transfer_type
    from `dbt`.`stg_envio_ga__transfer_actions`
    group by participant, transfer_type
    having count(*) > 1

)

select *
from validation_errors


