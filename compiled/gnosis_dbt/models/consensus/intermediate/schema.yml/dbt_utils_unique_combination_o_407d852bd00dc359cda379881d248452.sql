





with validation_errors as (

    select
        date, graffiti
    from (select * from `dbt`.`int_consensus_graffiti_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, graffiti
    having count(*) > 1

)

select *
from validation_errors


