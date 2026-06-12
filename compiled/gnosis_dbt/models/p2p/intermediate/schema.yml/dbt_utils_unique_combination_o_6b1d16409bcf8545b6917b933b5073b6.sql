





with validation_errors as (

    select
        visit_ended_at, peer_id
    from (select * from `dbt`.`int_p2p_discv4_peers` where toDate(visit_ended_at) >= today() - 7) dbt_subquery
    group by visit_ended_at, peer_id
    having count(*) > 1

)

select *
from validation_errors


