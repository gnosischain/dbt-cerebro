
    
    



select date
from (select * from `dbt`.`int_p2p_discv4_visits_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


