





with validation_errors as (

    select
        kpi_name, media_name
    from `dbt`.`fct_execution_mmm_baseline_latest`
    group by kpi_name, media_name
    having count(*) > 1

)

select *
from validation_errors


