





with validation_errors as (

    select
        date, source, target, edge_type
    from `dbt`.`fct_execution_account_counterparty_edges_daily`
    group by date, source, target, edge_type
    having count(*) > 1

)

select *
from validation_errors


