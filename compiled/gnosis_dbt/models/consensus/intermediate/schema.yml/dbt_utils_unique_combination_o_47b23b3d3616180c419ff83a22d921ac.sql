





with validation_errors as (

    select
        date, inclusion_delay
    from (select * from `dbt`.`int_consensus_attestations_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, inclusion_delay
    having count(*) > 1

)

select *
from validation_errors


