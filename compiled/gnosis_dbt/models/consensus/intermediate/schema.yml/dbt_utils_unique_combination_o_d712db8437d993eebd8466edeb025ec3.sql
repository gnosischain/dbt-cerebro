





with validation_errors as (

    select
        date, validator_index
    from (select * from `dbt`.`int_consensus_validators_proposer_rewards_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, validator_index
    having count(*) > 1

)

select *
from validation_errors


