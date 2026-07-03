





with validation_errors as (

    select
        date
    from `dbt`.`fct_execution_gnosis_app_gpay_migration_daily`
    group by date
    having count(*) > 1

)

select *
from validation_errors


