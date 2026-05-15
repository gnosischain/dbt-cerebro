



select
    1
from `dbt`.`fct_execution_gnosis_app_journeys_30d`

where not(lag_seconds lag_seconds > 0)

