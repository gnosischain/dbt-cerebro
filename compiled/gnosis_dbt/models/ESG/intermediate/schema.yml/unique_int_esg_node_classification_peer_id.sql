
    
    

select
    peer_id as unique_field,
    count(*) as n_records

from `dbt`.`int_esg_node_classification`
where peer_id is not null
group by peer_id
having count(*) > 1


