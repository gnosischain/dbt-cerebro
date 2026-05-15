





with validation_errors as (

    select
        date, container_address, ubo_address, token_address
    from (select * from `dbt`.`int_ubo_claims_balancer_v2_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, container_address, ubo_address, token_address
    having count(*) > 1

)

select *
from validation_errors


