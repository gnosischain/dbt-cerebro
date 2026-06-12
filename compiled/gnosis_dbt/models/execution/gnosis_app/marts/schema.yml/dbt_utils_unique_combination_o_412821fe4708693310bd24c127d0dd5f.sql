





with validation_errors as (

    select
        conversion_date, conversion_kind, user_pseudonym, conversion_ts, event_kind
    from (select * from `dbt`.`fct_execution_gnosis_app_journeys_30d` where toDate(conversion_date) >= today() - 7) dbt_subquery
    group by conversion_date, conversion_kind, user_pseudonym, conversion_ts, event_kind
    having count(*) > 1

)

select *
from validation_errors


