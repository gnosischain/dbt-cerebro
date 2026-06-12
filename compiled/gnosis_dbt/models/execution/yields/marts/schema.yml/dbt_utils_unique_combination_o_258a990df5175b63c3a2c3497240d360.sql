





with validation_errors as (

    select
        date, provider, pool_address
    from (select * from `dbt`.`fct_execution_yields_user_fee_collections_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, provider, pool_address
    having count(*) > 1

)

select *
from validation_errors


