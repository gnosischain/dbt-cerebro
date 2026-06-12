





with validation_errors as (

    select
        week, sector
    from (select * from `dbt`.`fct_execution_transactions_by_sector_weekly` where toDate(week) >= today() - 7) dbt_subquery
    group by week, sector
    having count(*) > 1

)

select *
from validation_errors


