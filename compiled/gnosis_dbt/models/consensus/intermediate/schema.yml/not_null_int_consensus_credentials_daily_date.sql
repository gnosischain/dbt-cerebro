
    
    



select date
from (select * from `dbt`.`int_consensus_credentials_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


