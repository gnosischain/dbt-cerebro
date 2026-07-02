





with validation_errors as (

    select
        week, symbol, cohort
    from `dbt`.`fct_revenue_holdings_cohorts_weekly`
    group by week, symbol, cohort
    having count(*) > 1

)

select *
from validation_errors


