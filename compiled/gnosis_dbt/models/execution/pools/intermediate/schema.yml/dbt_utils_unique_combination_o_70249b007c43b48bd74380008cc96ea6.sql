





with validation_errors as (

    select
        date, protocol, pool_address
    from (select * from `dbt`.`int_execution_pools_lps_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, protocol, pool_address
    having count(*) > 1

)

select *
from validation_errors


