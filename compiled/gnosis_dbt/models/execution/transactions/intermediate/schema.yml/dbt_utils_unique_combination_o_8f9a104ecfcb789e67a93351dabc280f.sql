





with validation_errors as (

    select
        project, month
    from (select * from `dbt`.`int_execution_transactions_by_project_alltime_state` where toDate(month) >= today() - 7) dbt_subquery
    group by project, month
    having count(*) > 1

)

select *
from validation_errors


