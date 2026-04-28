
    
    



select date
from (select * from `dbt`.`int_consensus_validators_deposits_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


