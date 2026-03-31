
    
    



select visit_ended_at
from (select * from `dbt`.`stg_nebula_discv4__visits` where toDate(visit_started_at) >= today() - 7) dbt_subquery
where visit_ended_at is null


