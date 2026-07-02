
    
    

select
    user_pseudonym as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_validators_withdrawal_addresses_distinct`
where user_pseudonym is not null
group by user_pseudonym
having count(*) > 1


