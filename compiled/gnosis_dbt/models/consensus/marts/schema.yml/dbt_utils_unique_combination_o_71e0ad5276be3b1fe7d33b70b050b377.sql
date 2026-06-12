





with validation_errors as (

    select
        date
    from (select * from `dbt`.`fct_consensus_attestations_performance_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date
    having count(*) > 1

)

select *
from validation_errors


