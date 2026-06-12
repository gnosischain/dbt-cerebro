





with validation_errors as (

    select
        week, kpi_name
    from (select * from `dbt`.`int_execution_mmm_kpis_weekly` where toDate(week) >= today() - 7) dbt_subquery
    group by week, kpi_name
    having count(*) > 1

)

select *
from validation_errors


