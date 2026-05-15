





with validation_errors as (

    select
        conversion_kind, event_kind
    from `dbt`.`fct_execution_gnosis_app_attribution_30d`
    group by conversion_kind, event_kind
    having count(*) > 1

)

select *
from validation_errors


