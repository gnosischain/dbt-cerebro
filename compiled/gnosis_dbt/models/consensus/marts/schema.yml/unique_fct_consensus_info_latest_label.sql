
    
    

select
    label as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_info_latest`
where label is not null
group by label
having count(*) > 1


