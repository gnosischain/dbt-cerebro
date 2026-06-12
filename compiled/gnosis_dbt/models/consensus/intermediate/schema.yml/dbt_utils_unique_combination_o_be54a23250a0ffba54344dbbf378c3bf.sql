





with validation_errors as (

    select
        date, status
    from (select * from `dbt`.`int_consensus_validators_status_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, status
    having count(*) > 1

)

select *
from validation_errors


