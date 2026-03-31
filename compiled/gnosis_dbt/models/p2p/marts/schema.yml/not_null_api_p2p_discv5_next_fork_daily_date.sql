
    
    



select date
from (select * from `dbt`.`api_p2p_discv5_next_fork_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


