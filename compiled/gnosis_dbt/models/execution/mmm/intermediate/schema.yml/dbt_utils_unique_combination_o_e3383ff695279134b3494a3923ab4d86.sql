





with validation_errors as (

    select
        week, kpi_name
    from `dbt`.`int_execution_mmm_kpis_weekly`
    group by week, kpi_name
    having count(*) > 1

)

select *
from validation_errors


