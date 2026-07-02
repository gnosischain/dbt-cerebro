
    
    

select
    fork_name as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_forks`
where fork_name is not null
group by fork_name
having count(*) > 1


