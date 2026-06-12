





with validation_errors as (

    select
        date, sector
    from (select * from `dbt`.`fct_execution_transactions_by_sector_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, sector
    having count(*) > 1

)

select *
from validation_errors


