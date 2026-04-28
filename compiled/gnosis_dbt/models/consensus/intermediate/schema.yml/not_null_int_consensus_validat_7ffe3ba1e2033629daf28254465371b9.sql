
    
    



select validator_index
from (select * from `dbt`.`int_consensus_validators_proposer_rewards_daily` where toDate(date) >= today() - 7) dbt_subquery
where validator_index is null


