





with validation_errors as (

    select
        month, stream_type, symbol, user
    from (select * from `dbt`.`int_revenue_fees_monthly_per_user` where toDate(month) >= today() - 7) dbt_subquery
    group by month, stream_type, symbol, user
    having count(*) > 1

)

select *
from validation_errors


