





with validation_errors as (

    select
        card, funder
    from `dbt`.`stg_envio_ga__pay_topups`
    group by card, funder
    having count(*) > 1

)

select *
from validation_errors


