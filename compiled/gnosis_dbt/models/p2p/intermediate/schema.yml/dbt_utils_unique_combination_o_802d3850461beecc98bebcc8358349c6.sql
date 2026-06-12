





with validation_errors as (

    select
        date, metric, label
    from (select * from `dbt`.`int_p2p_discv4_clients_daily` where toDate(date) >= today() - 7) dbt_subquery
    group by date, metric, label
    having count(*) > 1

)

select *
from validation_errors


