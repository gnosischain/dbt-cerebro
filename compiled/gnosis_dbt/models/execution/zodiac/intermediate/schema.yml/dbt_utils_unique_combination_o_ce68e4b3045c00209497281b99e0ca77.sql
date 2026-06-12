





with validation_errors as (

    select
        proxy_address
    from (select * from `dbt`.`int_execution_zodiac_module_proxies` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by proxy_address
    having count(*) > 1

)

select *
from validation_errors


