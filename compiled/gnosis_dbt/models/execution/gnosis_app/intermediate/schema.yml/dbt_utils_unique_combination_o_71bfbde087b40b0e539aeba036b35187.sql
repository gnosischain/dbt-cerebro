





with validation_errors as (

    select
        event_ts, event_kind, user_pseudonym, event_dedup_key
    from `dbt`.`int_execution_gnosis_app_events_chain_unified`
    group by event_ts, event_kind, user_pseudonym, event_dedup_key
    having count(*) > 1

)

select *
from validation_errors


