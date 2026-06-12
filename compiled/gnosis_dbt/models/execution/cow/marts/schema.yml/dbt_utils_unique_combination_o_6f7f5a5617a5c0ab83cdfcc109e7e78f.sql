





with validation_errors as (

    select
        date, solver
    from (select * from `dbt`.`fct_execution_cow_solvers_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, solver
    having count(*) > 1

)

select *
from validation_errors


