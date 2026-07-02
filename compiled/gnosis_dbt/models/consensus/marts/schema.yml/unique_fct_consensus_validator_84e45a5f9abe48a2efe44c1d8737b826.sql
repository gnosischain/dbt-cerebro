
    
    

select
    validator_index as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_validators_explorer_members_table`
where validator_index is not null
group by validator_index
having count(*) > 1


