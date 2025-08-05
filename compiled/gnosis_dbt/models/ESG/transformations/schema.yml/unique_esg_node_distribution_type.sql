
    
    

select
    type as unique_field,
    count(*) as n_records

from `dbt`.`esg_node_distribution`
where type is not null
group by type
having count(*) > 1


