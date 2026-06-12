





with validation_errors as (

    select
        source, date, target, edge_type
    from (select * from `dbt`.`fct_execution_account_counterparty_edges_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by source, date, target, edge_type
    having count(*) > 1

)

select *
from validation_errors


