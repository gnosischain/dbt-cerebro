





with validation_errors as (

    select
        date, protocol, container_address, ubo_address
    from (select * from `dbt`.`int_ubo_claims_aave_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, protocol, container_address, ubo_address
    having count(*) > 1

)

select *
from validation_errors


