
    
    



select date
from (select * from `dbt`.`fct_consensus_validators_dists_last_30_days` where toDate(date) >= today() - 7) dbt_subquery
where date is null


