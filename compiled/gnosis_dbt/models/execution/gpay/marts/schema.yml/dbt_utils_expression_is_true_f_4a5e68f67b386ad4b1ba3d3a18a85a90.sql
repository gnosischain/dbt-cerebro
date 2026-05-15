



select
    1
from `dbt`.`fct_execution_gpay_journeys_30d`

where not(lag_seconds > 0)

