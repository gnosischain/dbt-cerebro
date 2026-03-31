
    
    



select visit_started_at
from (select * from `dbt`.`stg_nebula_discv5__visits` where toDate(visit_started_at) >= today() - 7) dbt_subquery
where visit_started_at is null


