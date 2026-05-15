





with validation_errors as (

    select
        conversion_kind, identity_role, event_kind
    from `dbt`.`fct_execution_gpay_attribution_60d`
    group by conversion_kind, identity_role, event_kind
    having count(*) > 1

)

select *
from validation_errors


