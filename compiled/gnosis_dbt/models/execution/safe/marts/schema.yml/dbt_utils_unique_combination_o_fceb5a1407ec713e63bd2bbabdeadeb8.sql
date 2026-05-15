





with validation_errors as (

    select
        safe_user_pseudonym, owner_user_pseudonym
    from `dbt`.`fct_execution_safe_owner_pseudonyms`
    group by safe_user_pseudonym, owner_user_pseudonym
    having count(*) > 1

)

select *
from validation_errors


