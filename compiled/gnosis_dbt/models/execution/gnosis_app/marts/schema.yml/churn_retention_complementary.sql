



select
    1
from (select * from `dbt`.`fct_execution_gnosis_app_churn_monthly` where month > '2025-12-01' AND month < toStartOfMonth(today())) dbt_subquery

where not(churn_rate + retention_rate BETWEEN 80 AND 120)

