





with validation_errors as (

    select
        month, symbol, cohort
    from `dbt`.`fct_revenue_holdings_cohorts_monthly`
    group by month, symbol, cohort
    having count(*) > 1

)

select *
from validation_errors


