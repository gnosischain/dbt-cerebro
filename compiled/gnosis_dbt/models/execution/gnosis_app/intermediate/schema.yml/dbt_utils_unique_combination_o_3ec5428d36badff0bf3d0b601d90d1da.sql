





with validation_errors as (

    select
        address
    from `dbt`.`int_execution_gnosis_app_first_conversion`
    group by address
    having count(*) > 1

)

select *
from validation_errors


