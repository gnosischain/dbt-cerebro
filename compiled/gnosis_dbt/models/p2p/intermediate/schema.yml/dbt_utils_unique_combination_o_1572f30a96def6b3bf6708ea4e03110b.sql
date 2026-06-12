





with validation_errors as (

    select
        date
    from (select * from `dbt`.`int_p2p_discv5_visits_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date
    having count(*) > 1

)

select *
from validation_errors


