





with validation_errors as (

    select
        date, withdrawal_credentials
    from (select * from `dbt`.`int_consensus_validators_explorer_apy_dist_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, withdrawal_credentials
    having count(*) > 1

)

select *
from validation_errors


