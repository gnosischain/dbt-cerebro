





with validation_errors as (

    select
        project_id, event_name, event_time, insert_id
    from (select * from `dbt`.`stg_mixpanel_ga__events` where toDate(event_date) >= today() - 7) dbt_subquery
    group by project_id, event_name, event_time, insert_id
    having count(*) > 1

)

select *
from validation_errors


