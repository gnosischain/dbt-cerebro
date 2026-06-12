





with validation_errors as (

    select
        label, date
    from (select * from `dbt`.`int_consensus_deposits_withdrawals_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by label, date
    having count(*) > 1

)

select *
from validation_errors


