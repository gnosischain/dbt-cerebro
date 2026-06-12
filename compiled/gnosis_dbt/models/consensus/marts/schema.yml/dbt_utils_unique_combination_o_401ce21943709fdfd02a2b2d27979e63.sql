





with validation_errors as (

    select
        withdrawal_credentials, date
    from (select * from `dbt`.`fct_consensus_validators_explorer_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by withdrawal_credentials, date
    having count(*) > 1

)

select *
from validation_errors


