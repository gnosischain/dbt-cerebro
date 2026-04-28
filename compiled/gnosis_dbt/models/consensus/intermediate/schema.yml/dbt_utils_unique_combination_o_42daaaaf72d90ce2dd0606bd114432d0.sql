





with validation_errors as (

    select
        date, validator_index, role
    from `dbt`.`int_consensus_validators_consolidations_daily`
    group by date, validator_index, role
    having count(*) > 1

)

select *
from validation_errors


