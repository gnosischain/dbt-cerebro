





with validation_errors as (

    select
        event_date, user_pseudonym, event_ts, event_kind, event_dedup_key
    from (select * from `dbt`.`int_execution_gnosis_app_events_chain_unified` where toDate(event_date) >= today() - 7) dbt_subquery
    group by event_date, user_pseudonym, event_ts, event_kind, event_dedup_key
    having count(*) > 1

)

select *
from validation_errors


