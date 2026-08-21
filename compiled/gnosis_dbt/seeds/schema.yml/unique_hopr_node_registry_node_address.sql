
    
    

select
    node_address as unique_field,
    count(*) as n_records

from `dbt`.`hopr_node_registry`
where node_address is not null
group by node_address
having count(*) > 1


