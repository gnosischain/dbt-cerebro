





with validation_errors as (

    select
        user_pseudonym
    from `dbt`.`fct_execution_gnosis_app_gt_user_identities_public`
    group by user_pseudonym
    having count(*) > 1

)

select *
from validation_errors


