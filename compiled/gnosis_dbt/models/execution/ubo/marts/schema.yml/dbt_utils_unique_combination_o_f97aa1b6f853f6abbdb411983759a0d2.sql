





with validation_errors as (

    select
        date, container_address, token_address
    from (select * from `dbt`.`fct_ubo_known_containers_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, container_address, token_address
    having count(*) > 1

)

select *
from validation_errors


