





with validation_errors as (

    select
        date, bridge, source_chain, dest_chain, token, direction
    from (select * from `dbt`.`int_bridges_flows_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, bridge, source_chain, dest_chain, token, direction
    having count(*) > 1

)

select *
from validation_errors


