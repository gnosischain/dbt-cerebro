





with validation_errors as (

    select
        week, user_pseudonym
    from `dbt`.`fct_revenue_per_user_weekly`
    group by week, user_pseudonym
    having count(*) > 1

)

select *
from validation_errors


