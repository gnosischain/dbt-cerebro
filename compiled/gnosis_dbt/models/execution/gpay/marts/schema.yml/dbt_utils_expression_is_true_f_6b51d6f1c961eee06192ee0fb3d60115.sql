



select
    1
from `dbt`.`fct_execution_gpay_journeys_60d`

where not(lag_seconds > 0)

