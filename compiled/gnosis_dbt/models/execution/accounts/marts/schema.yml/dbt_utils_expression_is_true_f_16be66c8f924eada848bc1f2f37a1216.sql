



select
    1
from `dbt`.`fct_execution_account_profile_latest`

where not(first_seen_date first_seen_date IS NULL OR first_seen_date > toDate('1970-01-01'))

