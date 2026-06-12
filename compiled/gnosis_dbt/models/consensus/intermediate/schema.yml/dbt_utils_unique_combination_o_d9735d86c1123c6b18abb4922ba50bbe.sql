





with validation_errors as (

    select
        date, credentials_type
    from (select * from `dbt`.`int_consensus_credentials_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, credentials_type
    having count(*) > 1

)

select *
from validation_errors


