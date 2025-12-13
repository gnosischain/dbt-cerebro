
    
    

select
    crawl_id as unique_field,
    count(*) as n_records

from `dbt`.`int_esg_node_population_chao1`
where crawl_id is not null
group by crawl_id
having count(*) > 1


