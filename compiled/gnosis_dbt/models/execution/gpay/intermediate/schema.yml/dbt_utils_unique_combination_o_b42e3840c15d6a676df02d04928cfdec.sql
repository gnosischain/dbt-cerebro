





with validation_errors as (

    select
        address, identity_role, gp_safe
    from `dbt`.`int_execution_gpay_user_identity_bridge`
    group by address, identity_role, gp_safe
    having count(*) > 1

)

select *
from validation_errors


