





with validation_errors as (

    select
        source, target, edge_type
    from `dbt`.`fct_execution_account_counterparty_edges_latest`
    group by source, target, edge_type
    having count(*) > 1

)

select *
from validation_errors


