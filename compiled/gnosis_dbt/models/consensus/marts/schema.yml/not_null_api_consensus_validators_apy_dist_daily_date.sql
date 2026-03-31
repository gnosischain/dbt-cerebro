
    
    



select date
from (select * from `dbt`.`api_consensus_validators_apy_dist_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


