





with validation_errors as (

    select
        event_date, event_kind, user_pseudonym, event_ts, event_dedup_key
    from (select * from `dbt`.`int_execution_gnosis_app_user_events_unified` where toDate(event_date) >= today() - 7) dbt_subquery
    group by event_date, event_kind, user_pseudonym, event_ts, event_dedup_key
    having count(*) > 1

)

select *
from validation_errors


