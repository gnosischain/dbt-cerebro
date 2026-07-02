





with validation_errors as (

    select
        week, cohort
    from `dbt`.`fct_revenue_sdai_cohorts_weekly`
    group by week, cohort
    having count(*) > 1

)

select *
from validation_errors


