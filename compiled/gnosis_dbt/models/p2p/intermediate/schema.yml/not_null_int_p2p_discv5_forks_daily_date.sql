
    
    



select date
from (select * from `dbt`.`int_p2p_discv5_forks_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


