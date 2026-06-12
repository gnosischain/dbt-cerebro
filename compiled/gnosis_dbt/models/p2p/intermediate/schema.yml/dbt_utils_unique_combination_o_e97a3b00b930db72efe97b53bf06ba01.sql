





with validation_errors as (

    select
        date, label, fork
    from (select * from `dbt`.`int_p2p_discv5_forks_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, label, fork
    having count(*) > 1

)

select *
from validation_errors


