





with validation_errors as (

    select
        conversion_date, conversion_kind, user_pseudonym, identity_role, gp_safe, conversion_ts, conversion_dedup_key
    from (select * from `dbt`.`int_execution_gpay_conversions` where toDate(conversion_date) >= today() - 7) dbt_subquery
    group by conversion_date, conversion_kind, user_pseudonym, identity_role, gp_safe, conversion_ts, conversion_dedup_key
    having count(*) > 1

)

select *
from validation_errors


