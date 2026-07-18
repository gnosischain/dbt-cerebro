





with validation_errors as (

    select
        card
    from `dbt`.`int_execution_gnosis_app_gp_card_ga_link`
    group by card
    having count(*) > 1

)

select *
from validation_errors


