





with validation_errors as (

    select
        month, user_pseudonym
    from `dbt`.`fct_revenue_per_user_monthly`
    group by month, user_pseudonym
    having count(*) > 1

)

select *
from validation_errors


