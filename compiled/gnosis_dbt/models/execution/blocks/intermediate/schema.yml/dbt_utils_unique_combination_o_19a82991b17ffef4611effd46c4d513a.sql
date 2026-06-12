





with validation_errors as (

    select
        date, client, version
    from (select * from `dbt`.`int_execution_blocks_clients_version_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, client, version
    having count(*) > 1

)

select *
from validation_errors


