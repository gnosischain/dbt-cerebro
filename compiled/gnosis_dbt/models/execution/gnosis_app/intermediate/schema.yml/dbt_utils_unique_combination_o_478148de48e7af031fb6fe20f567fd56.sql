





with validation_errors as (

    select
        address, user_pseudonym
    from `dbt`.`int_execution_gnosis_app_user_identity_bridge`
    group by address, user_pseudonym
    having count(*) > 1

)

select *
from validation_errors


