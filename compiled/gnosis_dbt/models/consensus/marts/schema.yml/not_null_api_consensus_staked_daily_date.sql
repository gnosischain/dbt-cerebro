
    
    



select date
from (select * from `dbt`.`api_consensus_staked_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


