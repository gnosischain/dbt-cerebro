





with validation_errors as (

    select
        date, role
    from `dbt`.`fct_consensus_consolidations_daily`
    group by date, role
    having count(*) > 1

)

select *
from validation_errors


