





with validation_errors as (

    select
        conversion_ts, conversion_kind, user_pseudonym, identity_role, conversion_dedup_key
    from `dbt`.`int_execution_gpay_conversions`
    group by conversion_ts, conversion_kind, user_pseudonym, identity_role, conversion_dedup_key
    having count(*) > 1

)

select *
from validation_errors


