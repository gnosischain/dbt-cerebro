





with jobs as (
  select
    job_name,
    job_id,
    job_run_id,
    
min(coalesce(
        parseDateTimeBestEffortOrNull(toString(run_started_at), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    ))
 as job_run_started_at,
    
max(coalesce(
        parseDateTimeBestEffortOrNull(toString(run_completed_at), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    ))
 as job_run_completed_at,
    
    coalesce(dateDiff('second', coalesce(
        parseDateTimeBestEffortOrNull(toString(
min(coalesce(
        parseDateTimeBestEffortOrNull(toString(run_started_at), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    ))
), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    ), coalesce(
        parseDateTimeBestEffortOrNull(toString(
max(coalesce(
        parseDateTimeBestEffortOrNull(toString(run_completed_at), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    ))
), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    )), 0)::Nullable(Int32)

 as job_run_execution_time
  from `elementary`.`dbt_invocations`
  where job_id is not null
  group by job_name, job_id, job_run_id
)

select
  job_name as name,
  job_id as id,
  job_run_id as run_id,
  job_run_started_at as run_started_at,
  job_run_completed_at as run_completed_at,
  job_run_execution_time as run_execution_time
from jobs